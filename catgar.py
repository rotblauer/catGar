#!/usr/bin/env python3
"""catGar — sync Garmin watch data to InfluxDB.

Usage:
    python catgar.py              # sync since last sync (or today if first run)
    python catgar.py 2024-01-15   # sync a specific date
    python catgar.py --days 7     # sync last 7 days
    python catgar.py --backfill   # initial backfill of all available data
"""

import argparse
import json
import logging
import os
import statistics
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from garminconnect import Garmin
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules
from influxdb_client.rest import ApiException
from requests.exceptions import HTTPError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("catgar")

# Default path for last-sync state file (next to this script)
DEFAULT_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".last_sync")

# Maximum number of days to look back for a full backfill
BACKFILL_MAX_DAYS = 365 * 5


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def get_config():
    """Return a dict of required config values from environment."""
    keys = {
        "GARMIN_EMAIL": None,
        "GARMIN_PASSWORD": None,
        "INFLUXDB_URL": "http://localhost:8086",
        "INFLUXDB_TOKEN": None,
        "INFLUXDB_ORG": None,
        "INFLUXDB_BUCKET": "garmin",
    }
    cfg = {}
    for key, default in keys.items():
        val = os.environ.get(key, default)
        if val is None:
            log.error("Missing required environment variable: %s", key)
            sys.exit(1)
        cfg[key] = val
    return cfg


def ensure_bucket(influx_client, bucket, org):
    """Ensure the target InfluxDB bucket exists with infinite retention.

    Creates the bucket with no expiration (``every_seconds=0``) if missing.
    If the bucket already exists, logs a warning when its retention policy
    is not set to infinite so the operator can adjust it manually.
    """
    buckets_api = influx_client.buckets_api()
    existing = buckets_api.find_bucket_by_name(bucket)
    if existing:
        rules = existing.retention_rules or []
        for rule in rules:
            if rule.every_seconds and rule.every_seconds > 0:
                log.warning(
                    "Bucket '%s' has a finite retention of %d seconds. "
                    "Data may be dropped. Consider setting retention to infinite (0).",
                    bucket,
                    rule.every_seconds,
                )
        return
    try:
        retention = BucketRetentionRules(type="expire", every_seconds=0)
        buckets_api.create_bucket(
            bucket_name=bucket, org=org, retention_rules=[retention],
        )
        log.info("Created InfluxDB bucket '%s' with infinite retention.", bucket)
    except ApiException as exc:
        log.error("Failed to create bucket '%s': %s", bucket, exc)
        influx_client.close()
        sys.exit(1)


# ---------------------------------------------------------------------------
# Garmin data → InfluxDB points
# ---------------------------------------------------------------------------

def _safe_float(val, field_name, measurement):
    """Convert *val* to float, logging a warning on failure instead of raising."""
    try:
        return float(val)
    except (ValueError, TypeError):
        log.warning(
            "Could not convert %s=%r to float for measurement '%s'; skipping field.",
            field_name, val, measurement,
        )
        return None


# Keys that are metadata / non-numeric and should never be treated as fields.
_IGNORED_GENERIC_KEYS = frozenset({
    "calendarDate", "startTimestampGMT", "endTimestampGMT",
    "startTimestampLocal", "endTimestampLocal",
    "userProfilePK", "startOfDayGMT", "startOfDayLocal",
    "userDailySummaryId",
    # Wellness / daily-stats timestamp & string fields
    "wellnessStartTimeGmt", "wellnessStartTimeLocal",
    "wellnessEndTimeGmt", "wellnessEndTimeLocal",
    "source", "stressQualifier",
    # SpO2 / respiration timestamp fields
    "latestSpo2ReadingTimeGmt", "latestSpo2ReadingTimeLocal",
    "latestRespirationTimeGMT",
    "latestSpO2TimestampGMT", "latestSpO2TimestampLocal",
    # Sleep-related timestamp fields
    "tomorrowSleepStartTimestampGMT", "tomorrowSleepEndTimestampGMT",
    "tomorrowSleepStartTimestampLocal", "tomorrowSleepEndTimestampLocal",
    # Body composition date fields
    "startDate", "endDate",
})


def _collect_extra_fields(data, known_keys, measurement):
    """Return a dict of ``{key: float_value}`` for numeric fields in *data*
    that are not in *known_keys*.

    This allows newly-added Garmin fields to be captured automatically.
    A log message is emitted once per unknown key so operators can add it
    to the explicit field map if desired.
    """
    extras = {}
    if not isinstance(data, dict):
        return extras
    for key, val in data.items():
        if key in known_keys or key in _IGNORED_GENERIC_KEYS:
            continue
        if val is None or isinstance(val, (dict, list)):
            continue
        fval = _safe_float(val, key, measurement)
        if fval is not None:
            log.debug(
                "Discovered extra numeric field '%s'=%s in '%s'",
                key, fval, measurement,
            )
            extras[key] = fval
    return extras

def build_daily_stats_points(stats, day_str):
    """Convert Garmin daily stats dict into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    field_map = {
        "totalSteps": "steps",
        "totalDistanceMeters": "distance_meters",
        "activeKilocalories": "active_kcal",
        "totalKilocalories": "total_kcal",
        "restingHeartRate": "resting_hr",
        "maxHeartRate": "max_hr",
        "minHeartRate": "min_hr",
        "averageHeartRate": "avg_hr",
        "moderateIntensityMinutes": "moderate_intensity_min",
        "vigorousIntensityMinutes": "vigorous_intensity_min",
        "floorsAscended": "floors_ascended",
        "floorsDescended": "floors_descended",
        "averageStressLevel": "avg_stress",
        "maxStressLevel": "max_stress",
        "bodyBatteryChargedValue": "body_battery_charged",
        "bodyBatteryDrainedValue": "body_battery_drained",
        "bodyBatteryHighestValue": "body_battery_high",
        "bodyBatteryLowestValue": "body_battery_low",
    }

    for garmin_key, influx_field in field_map.items():
        val = stats.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "daily_stats")
            if fval is None:
                continue
            p = (
                Point("daily_stats")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(stats, set(field_map), "daily_stats").items():
        p = Point("daily_stats").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_sleep_points(sleep_data, day_str):
    """Convert Garmin sleep data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    summary = sleep_data.get("dailySleepDTO", {})

    field_map = {
        "sleepTimeSeconds": "sleep_time_sec",
        "deepSleepSeconds": "deep_sleep_sec",
        "lightSleepSeconds": "light_sleep_sec",
        "remSleepSeconds": "rem_sleep_sec",
        "awakeSleepSeconds": "awake_sec",
        "averageSpO2Value": "avg_spo2",
        "lowestSpO2Value": "lowest_spo2",
        "averageRespirationValue": "avg_respiration",
        "lowestRespirationValue": "lowest_respiration",
        "highestRespirationValue": "highest_respiration",
        "averageSpO2HRSleep": "avg_hr_sleep",
        "sleepScores": None,  # handled separately
    }

    for garmin_key, influx_field in field_map.items():
        if influx_field is None:
            continue
        val = summary.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "sleep")
            if fval is None:
                continue
            p = (
                Point("sleep")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    # Sleep scores (nested object)
    scores = summary.get("sleepScores", {})
    if scores:
        for score_key in ("overall", "totalDuration", "stress", "revitalizationScore"):
            score_obj = scores.get(score_key)
            if isinstance(score_obj, dict):
                val = score_obj.get("value")
            else:
                val = score_obj
            if val is not None:
                fval = _safe_float(val, f"score_{score_key}", "sleep")
                if fval is None:
                    continue
                p = (
                    Point("sleep")
                    .time(ts, WritePrecision.S)
                    .field(f"score_{score_key}", fval)
                )
                points.append(p)

    for key, fval in _collect_extra_fields(summary, set(field_map), "sleep").items():
        p = Point("sleep").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_heart_rate_points(hr_data, day_str):
    """Convert Garmin heart-rate data into InfluxDB Point objects."""
    points = []

    for entry in (hr_data or []):
        if not isinstance(entry, dict):
            continue
        hr_values = entry.get("heartRateValues")
        if not hr_values or not isinstance(hr_values, (list, tuple)):
            continue
        for pair in hr_values:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            ts_ms, hr = pair
            if hr is None or ts_ms is None:
                continue
            p = (
                Point("heart_rate")
                .time(int(ts_ms), WritePrecision.MS)
                .field("bpm", int(hr))
            )
            points.append(p)

    return points


def build_activity_points(activities):
    """Convert Garmin activities list into InfluxDB Point objects."""
    points = []

    for act in (activities or []):
        ts_str = act.get("startTimeLocal") or act.get("startTimeGMT")
        if not ts_str:
            continue
        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            continue

        act_type = act.get("activityType", {}).get("typeKey", "unknown")
        act_name = act.get("activityName", "")

        p = Point("activity").tag("type", act_type).tag("name", act_name).time(ts, WritePrecision.S)

        field_map = {
            "distance": "distance_meters",
            "duration": "duration_sec",
            "elapsedDuration": "elapsed_sec",
            "movingDuration": "moving_sec",
            "averageHR": "avg_hr",
            "maxHR": "max_hr",
            "calories": "calories",
            "averageSpeed": "avg_speed",
            "maxSpeed": "max_speed",
            "elevationGain": "elevation_gain",
            "elevationLoss": "elevation_loss",
            "averageRunningCadenceInStepsPerMinute": "avg_cadence",
            "steps": "steps",
            "vO2MaxValue": "vo2max",
            "avgPower": "avg_power",
            "maxPower": "max_power",
            "trainingEffectLabel": None,
        }

        has_fields = False
        for garmin_key, influx_field in field_map.items():
            if influx_field is None:
                continue
            val = act.get(garmin_key)
            if val is not None:
                fval = _safe_float(val, garmin_key, "activity")
                if fval is None:
                    continue
                p = p.field(influx_field, fval)
                has_fields = True

        if has_fields:
            points.append(p)

    return points


def build_activity_detail_points(detail_data, activity_id, act_type, act_name, ts):
    """Convert Garmin activity detail (``get_activity``) into InfluxDB Points.

    Captures enriched per-activity metrics not available from the activities
    list endpoint, such as training effect, performance condition, and
    normalized power.
    """
    points = []
    if not detail_data or not isinstance(detail_data, dict):
        return points

    summary = detail_data.get("summaryDTO") or detail_data
    p = (
        Point("activity_detail")
        .tag("type", act_type)
        .tag("name", act_name)
        .tag("activity_id", str(activity_id))
        .time(ts, WritePrecision.S)
    )

    field_map = {
        "trainingEffect": "training_effect_aerobic",
        "anaerobicTrainingEffect": "training_effect_anaerobic",
        "aerobicTrainingEffectMessage": None,
        "anaerobicTrainingEffectMessage": None,
        "performanceCondition": "performance_condition",
        "lactateThreshold": "lactate_threshold",
        "normalizedPower": "normalized_power",
        "groundContactTime": "ground_contact_time",
        "groundContactBalanceLeft": "ground_contact_balance_left",
        "strideLength": "stride_length",
        "verticalOscillation": "vertical_oscillation",
        "verticalRatio": "vertical_ratio",
        "trainingStressScore": "training_stress_score",
        "intensityFactor": "intensity_factor",
        "functionalThresholdPower": "ftp",
        "minTemperature": "min_temperature",
        "maxTemperature": "max_temperature",
        "minElevation": "min_elevation",
        "maxElevation": "max_elevation",
        "maxRunCadence": "max_cadence",
        "maxBikeCadence": "max_bike_cadence",
        "lapCount": "lap_count",
        "waterEstimated": "water_estimated_ml",
        "directWorkoutFeel": None,
        "directWorkoutRpe": None,
    }

    has_fields = False
    for garmin_key, influx_field in field_map.items():
        if influx_field is None:
            continue
        val = summary.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "activity_detail")
            if fval is None:
                continue
            p = p.field(influx_field, fval)
            has_fields = True

    if has_fields:
        points.append(p)

    return points


def build_activity_track_points(detail_data, activity_id, act_type, act_name, ts):
    """Convert Garmin activity details (``get_activity_details``) GPS data into InfluxDB Points.

    Extracts high-resolution GPS track points from the ``activityDetailMetrics``
    array using the ``metricDescriptors`` to locate ``directLatitude`` and
    ``directLongitude`` indices.  Creates one ``activity_track`` point per
    GPS sample, enabling full-resolution route reconstruction.
    """
    points = []
    if not detail_data or not isinstance(detail_data, dict):
        return points

    descriptors = detail_data.get("metricDescriptors")
    metrics_list = detail_data.get("activityDetailMetrics")
    if not descriptors or not metrics_list:
        return points

    # Build index mapping from descriptor key to array position.
    key_to_idx = {}
    for desc in descriptors:
        key = desc.get("key")
        idx = desc.get("metricsIndex")
        if key is not None and idx is not None:
            key_to_idx[key] = idx

    lat_idx = key_to_idx.get("directLatitude")
    lon_idx = key_to_idx.get("directLongitude")
    if lat_idx is None or lon_idx is None:
        return points

    for point_num, entry in enumerate(metrics_list):
        metrics = entry.get("metrics") if isinstance(entry, dict) else None
        if not metrics:
            continue
        if lat_idx >= len(metrics) or lon_idx >= len(metrics):
            continue

        raw_lat = metrics[lat_idx]
        raw_lon = metrics[lon_idx]
        lat = _safe_float(raw_lat, "directLatitude", "activity_track")
        lon = _safe_float(raw_lon, "directLongitude", "activity_track")
        if lat is None or lon is None:
            continue

        p = (
            Point("activity_track")
            .tag("activity_id", str(activity_id))
            .tag("type", act_type)
            .tag("name", act_name)
            .tag("point_idx", str(point_num))
            .time(ts, WritePrecision.S)
            .field("lat", lat)
            .field("lon", lon)
        )

        # Capture additional numeric metrics available at this track point.
        for desc_key, desc_idx in key_to_idx.items():
            if desc_key in ("directLatitude", "directLongitude"):
                continue
            if desc_idx >= len(metrics):
                continue
            val = metrics[desc_idx]
            if val is None:
                continue
            fval = _safe_float(val, desc_key, "activity_track")
            if fval is not None:
                p = p.field(desc_key, fval)

        points.append(p)

    return points


def build_activity_split_points(splits_data, activity_id, act_type, act_name, ts):
    """Convert Garmin activity splits into InfluxDB Points.

    Creates one point per split/lap with distance, duration, pace, HR, and
    other per-split metrics.
    """
    points = []
    if not splits_data or not isinstance(splits_data, dict):
        return points

    lap_list = splits_data.get("lapDTOs") or splits_data.get("splitSummaries") or []
    if not isinstance(lap_list, list):
        return points

    for idx, lap in enumerate(lap_list):
        if not isinstance(lap, dict):
            continue

        p = (
            Point("activity_split")
            .tag("type", act_type)
            .tag("name", act_name)
            .tag("activity_id", str(activity_id))
            .tag("split_num", str(idx + 1))
            .time(ts, WritePrecision.S)
        )

        field_map = {
            "distance": "distance_meters",
            "duration": "duration_sec",
            "movingDuration": "moving_sec",
            "averageHR": "avg_hr",
            "maxHR": "max_hr",
            "averageSpeed": "avg_speed",
            "maxSpeed": "max_speed",
            "calories": "calories",
            "elevationGain": "elevation_gain",
            "elevationLoss": "elevation_loss",
            "averageRunCadence": "avg_cadence",
            "maxRunCadence": "max_cadence",
            "averagePower": "avg_power",
            "maxPower": "max_power",
            "startLatitude": "start_lat",
            "startLongitude": "start_lon",
            "endLatitude": "end_lat",
            "endLongitude": "end_lon",
            "totalExerciseReps": "total_reps",
            "messageIndex": None,
        }

        has_fields = False
        for garmin_key, influx_field in field_map.items():
            if influx_field is None:
                continue
            val = lap.get(garmin_key)
            if val is not None:
                fval = _safe_float(val, garmin_key, "activity_split")
                if fval is None:
                    continue
                p = p.field(influx_field, fval)
                has_fields = True

        if has_fields:
            points.append(p)

    return points


def build_activity_hr_zone_points(hr_zones_data, activity_id, act_type, act_name, ts):
    """Convert Garmin activity HR-zone data into InfluxDB Points.

    Creates one point per heart-rate zone with time-in-zone and zone boundaries.
    """
    points = []
    if not hr_zones_data or not isinstance(hr_zones_data, (list, dict)):
        return points

    zones = hr_zones_data
    if isinstance(hr_zones_data, dict):
        zones = hr_zones_data.get("hrTimeInZones") or hr_zones_data.get("heartRateZones") or []

    if not isinstance(zones, list):
        return points

    for zone in zones:
        if not isinstance(zone, dict):
            continue

        zone_num = zone.get("zoneNumber") or zone.get("zone")
        if zone_num is None:
            continue

        p = (
            Point("activity_hr_zone")
            .tag("type", act_type)
            .tag("name", act_name)
            .tag("activity_id", str(activity_id))
            .tag("zone", str(zone_num))
            .time(ts, WritePrecision.S)
        )

        field_map = {
            "secsInZone": "secs_in_zone",
            "zoneLowBoundary": "zone_low_bpm",
            "zoneHighBoundary": "zone_high_bpm",
        }

        has_fields = False
        for garmin_key, influx_field in field_map.items():
            val = zone.get(garmin_key)
            if val is not None:
                fval = _safe_float(val, garmin_key, "activity_hr_zone")
                if fval is None:
                    continue
                p = p.field(influx_field, fval)
                has_fields = True

        if has_fields:
            points.append(p)

    return points


def build_activity_weather_points(weather_data, activity_id, act_type, act_name, ts):
    """Convert Garmin activity weather data into InfluxDB Points."""
    points = []
    if not weather_data or not isinstance(weather_data, dict):
        return points

    p = (
        Point("activity_weather")
        .tag("type", act_type)
        .tag("name", act_name)
        .tag("activity_id", str(activity_id))
        .time(ts, WritePrecision.S)
    )

    field_map = {
        "temperature": "temperature_c",
        "apparentTemperature": "feels_like_c",
        "dewPoint": "dew_point_c",
        "relativeHumidity": "humidity_pct",
        "windDirection": "wind_direction_deg",
        "windSpeed": "wind_speed_mps",
        "windGust": "wind_gust_mps",
        "weatherTypeDTO": None,
    }

    has_fields = False
    for garmin_key, influx_field in field_map.items():
        if influx_field is None:
            continue
        val = weather_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "activity_weather")
            if fval is None:
                continue
            p = p.field(influx_field, fval)
            has_fields = True

    if has_fields:
        points.append(p)

    return points


def build_body_composition_points(body_data, day_str):
    """Convert Garmin body composition data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not body_data:
        return points

    field_map = {
        "weight": "weight_grams",
        "bmi": "bmi",
        "bodyFat": "body_fat_pct",
        "bodyWater": "body_water_pct",
        "muscleMass": "muscle_mass_grams",
        "skeletalMuscleMass": "skeletal_muscle_mass_grams",
        "boneMass": "bone_mass_grams",
        "metabolicAge": "metabolic_age",
        "visceralFat": "visceral_fat",
        "weightChange": "weight_change",
        "physiqueRating": "physique_rating",
    }

    for garmin_key, influx_field in field_map.items():
        val = body_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "body_composition")
            if fval is None:
                continue
            p = (
                Point("body_composition")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(body_data, set(field_map), "body_composition").items():
        p = Point("body_composition").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_respiration_points(resp_data, day_str):
    """Convert Garmin respiration data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not resp_data:
        return points

    field_map = {
        "avgWakingRespirationValue": "avg_waking_respiration",
        "highestRespirationValue": "highest_respiration",
        "lowestRespirationValue": "lowest_respiration",
        "avgSleepRespirationValue": "avg_sleep_respiration",
    }

    for garmin_key, influx_field in field_map.items():
        val = resp_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "respiration")
            if fval is None:
                continue
            p = (
                Point("respiration")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(resp_data, set(field_map), "respiration").items():
        p = Point("respiration").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_spo2_points(spo2_data, day_str):
    """Convert Garmin SpO2 data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not spo2_data:
        return points

    _known_spo2_keys = {"averageSpO2", "lowestSpO2", "latestSpO2"}
    for key in _known_spo2_keys:
        val = spo2_data.get(key)
        if val is not None:
            fval = _safe_float(val, key, "spo2")
            if fval is None:
                continue
            p = (
                Point("spo2")
                .time(ts, WritePrecision.S)
                .field(key, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(spo2_data, _known_spo2_keys, "spo2").items():
        p = Point("spo2").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_stress_points(stress_data, day_str):
    """Convert Garmin stress data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not stress_data:
        return points

    field_map = {
        "avgStressLevel": "avg_stress",
        "maxStressLevel": "max_stress",
        "totalStressDuration": "total_stress_duration",
        "lowStressDuration": "low_stress_duration",
        "mediumStressDuration": "medium_stress_duration",
        "highStressDuration": "high_stress_duration",
        "totalRestStressDuration": "rest_stress_duration",
    }

    for garmin_key, influx_field in field_map.items():
        val = stress_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "stress")
            if fval is None:
                continue
            p = (
                Point("stress")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(stress_data, set(field_map), "stress").items():
        p = Point("stress").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_hrv_points(hrv_data, day_str):
    """Convert Garmin HRV (Heart Rate Variability) data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not hrv_data:
        return points

    summary = hrv_data.get("hrvSummary", hrv_data)

    field_map = {
        "weeklyAvg": "weekly_avg",
        "lastNight": "last_night",
        "lastNightAvg": "last_night_avg",
        "lastNight5MinHigh": "last_night_5min_high",
        "baseline": None,  # nested, handled below
        "status": None,  # string, skip
    }

    for garmin_key, influx_field in field_map.items():
        if influx_field is None:
            continue
        val = summary.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "hrv")
            if fval is None:
                continue
            p = (
                Point("hrv")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    baseline = summary.get("baseline", {})
    if isinstance(baseline, dict):
        for bkey in ("lowUpper", "balancedLow", "balancedUpper"):
            val = baseline.get(bkey)
            if val is not None:
                fval = _safe_float(val, f"baseline_{bkey}", "hrv")
                if fval is not None:
                    p = (
                        Point("hrv")
                        .time(ts, WritePrecision.S)
                        .field(f"baseline_{bkey}", fval)
                    )
                    points.append(p)

    for key, fval in _collect_extra_fields(summary, set(field_map), "hrv").items():
        p = Point("hrv").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_hydration_points(hydration_data, day_str):
    """Convert Garmin hydration data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not hydration_data:
        return points

    field_map = {
        "valueInML": "intake_ml",
        "goalInML": "goal_ml",
        "sweatLossInML": "sweat_loss_ml",
    }

    for garmin_key, influx_field in field_map.items():
        val = hydration_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "hydration")
            if fval is None:
                continue
            p = (
                Point("hydration")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(hydration_data, set(field_map), "hydration").items():
        p = Point("hydration").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_training_readiness_points(readiness_data, day_str):
    """Convert Garmin training readiness data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not readiness_data:
        return points

    field_map = {
        "score": "score",
        "sleepScore": "sleep_score",
        "recoveryTime": "recovery_time",
        "acuteLoad": "acute_load",
        "hrvStatus": "hrv_status",
        "trainingLoad": "training_load",
    }

    for garmin_key, influx_field in field_map.items():
        val = readiness_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "training_readiness")
            if fval is None:
                continue
            p = (
                Point("training_readiness")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(readiness_data, set(field_map), "training_readiness").items():
        p = Point("training_readiness").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_training_status_points(status_data, day_str):
    """Convert Garmin training status data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not status_data:
        return points

    field_map = {
        "trainingLoadBalance": "load_balance",
        "ltTimestamp": "lt_timestamp",
        "vo2MaxValue": "vo2max",
        "loadFocus": "load_focus",
        "lactateThresholdHeartRate": "lt_heart_rate",
        "lactateThresholdSpeed": "lt_speed",
    }

    for garmin_key, influx_field in field_map.items():
        val = status_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "training_status")
            if fval is None:
                continue
            p = (
                Point("training_status")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(status_data, set(field_map), "training_status").items():
        p = Point("training_status").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_max_metrics_points(metrics_data, day_str):
    """Convert Garmin max metrics (VO2 max, etc.) into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not metrics_data:
        return points

    # The response may contain a list of metric entries or a dict wrapper.
    if isinstance(metrics_data, list):
        entries = metrics_data
    elif "maxMetrics" in metrics_data:
        entries = metrics_data.get("maxMetrics", [])
    else:
        entries = [metrics_data]

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        sport = entry.get("sport", entry.get("metricsType", "generic"))

        field_map = {
            "vo2MaxPreciseValue": "vo2max_precise",
            "vo2MaxValue": "vo2max",
            "fitnessAge": "fitness_age",
            "fitnessAgeDescription": None,  # string
        }

        has_fields = False
        p = Point("max_metrics").tag("sport", str(sport)).time(ts, WritePrecision.S)

        for garmin_key, influx_field in field_map.items():
            if influx_field is None:
                continue
            val = entry.get(garmin_key)
            if val is not None:
                fval = _safe_float(val, garmin_key, "max_metrics")
                if fval is None:
                    continue
                p = p.field(influx_field, fval)
                has_fields = True

        for key, fval in _collect_extra_fields(entry, set(field_map) | {"sport", "metricsType"}, "max_metrics").items():
            p = p.field(key, fval)
            has_fields = True

        if has_fields:
            points.append(p)

    return points


def build_endurance_score_points(score_data, day_str):
    """Convert Garmin endurance score data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not score_data:
        return points

    field_map = {
        "overallScore": "overall_score",
        "enduranceScore": "endurance_score",
    }

    for garmin_key, influx_field in field_map.items():
        val = score_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "endurance_score")
            if fval is None:
                continue
            p = (
                Point("endurance_score")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(score_data, set(field_map), "endurance_score").items():
        p = Point("endurance_score").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_hill_score_points(score_data, day_str):
    """Convert Garmin hill score data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not score_data:
        return points

    field_map = {
        "overallScore": "overall_score",
        "hillScore": "hill_score",
    }

    for garmin_key, influx_field in field_map.items():
        val = score_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "hill_score")
            if fval is None:
                continue
            p = (
                Point("hill_score")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(score_data, set(field_map), "hill_score").items():
        p = Point("hill_score").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_fitnessage_points(age_data, day_str):
    """Convert Garmin fitness age data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not age_data:
        return points

    field_map = {
        "fitnessAge": "fitness_age",
        "chronologicalAge": "chronological_age",
        "bmi": "bmi",
        "healthyBmiTop": "healthy_bmi_top",
        "healthyBmiBottom": "healthy_bmi_bottom",
        "vigorousMinutes": "vigorous_minutes",
        "vigorousMinutesGoal": "vigorous_minutes_goal",
        "restingHr": "resting_hr",
        "restingHrGoal": "resting_hr_goal",
    }

    for garmin_key, influx_field in field_map.items():
        val = age_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "fitness_age")
            if fval is None:
                continue
            p = (
                Point("fitness_age")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(age_data, set(field_map), "fitness_age").items():
        p = Point("fitness_age").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


def build_floors_points(floors_data, day_str):
    """Convert Garmin floors data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not floors_data:
        return points

    field_map = {
        "floorsAscended": "floors_ascended",
        "floorsDescended": "floors_descended",
        "floorsAscendedGoal": "floors_ascended_goal",
    }

    for garmin_key, influx_field in field_map.items():
        val = floors_data.get(garmin_key)
        if val is not None:
            fval = _safe_float(val, garmin_key, "floors")
            if fval is None:
                continue
            p = (
                Point("floors")
                .time(ts, WritePrecision.S)
                .field(influx_field, fval)
            )
            points.append(p)

    for key, fval in _collect_extra_fields(floors_data, set(field_map), "floors").items():
        p = Point("floors").time(ts, WritePrecision.S).field(key, fval)
        points.append(p)

    return points


# ---------------------------------------------------------------------------
# Last-sync state helpers
# ---------------------------------------------------------------------------

def read_last_sync(state_file=DEFAULT_STATE_FILE):
    """Read the last successful sync date from the state file.

    Returns a date object, or None if no previous sync recorded.
    """
    path = Path(state_file)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return datetime.strptime(data["last_sync"], "%Y-%m-%d").date()
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def write_last_sync(sync_date, state_file=DEFAULT_STATE_FILE):
    """Write the last successful sync date to the state file."""
    path = Path(state_file)
    path.write_text(json.dumps({"last_sync": sync_date.strftime("%Y-%m-%d")}))


# ---------------------------------------------------------------------------
# Backfill helpers
# ---------------------------------------------------------------------------

def _probe_date(garmin_client, day_str):
    """Return True if Garmin has any data for *day_str*."""
    try:
        stats = garmin_client.get_stats(day_str)
        if stats and any(stats.get(k) is not None for k in ("totalSteps", "totalDistanceMeters", "restingHeartRate")):
            return True
    except HTTPError as exc:
        resp = getattr(exc, "response", None)
        if getattr(resp, "status_code", None) == 404:
            return False
    except Exception:
        pass
    return False


def find_oldest_available_date(garmin_client, earliest, latest):
    """Binary search for the oldest date with Garmin data.

    Instead of checking every day from *earliest* to *latest*, this narrows
    the window in O(log n) probes, then returns the first date that has data.
    Returns *latest* if no data is found in the entire range.
    """
    low = 0
    high = (latest - earliest).days

    if high <= 0:
        if _probe_date(garmin_client, latest.strftime("%Y-%m-%d")):
            return latest
        return latest

    # Quick check: if the earliest date already has data, return it
    if _probe_date(garmin_client, earliest.strftime("%Y-%m-%d")):
        return earliest

    # Quick check: if the latest date has no data, nothing to backfill
    if not _probe_date(garmin_client, latest.strftime("%Y-%m-%d")):
        return latest

    while low < high:
        mid = (low + high) // 2
        mid_date = earliest + timedelta(days=mid)
        if _probe_date(garmin_client, mid_date.strftime("%Y-%m-%d")):
            high = mid
        else:
            low = mid + 1

    return earliest + timedelta(days=low)


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def fetch_and_write(garmin_client, influx_write_api, bucket, org, day_str):
    """Fetch all Garmin data for *day_str* and write to InfluxDB.

    Returns ``(total_points, errors, counts)`` where *counts* is a dict
    mapping measurement name to the number of points written.
    """
    total = 0
    errors = []
    counts = {}

    def _is_no_data_not_found(exc):
        if isinstance(exc, ApiException):
            return False
        if isinstance(exc, HTTPError):
            resp = getattr(exc, "response", None)
            return getattr(resp, "status_code", None) == 404
        return False

    collectors = [
        ("daily stats", lambda: build_daily_stats_points(garmin_client.get_stats(day_str), day_str)),
        ("sleep", lambda: build_sleep_points(garmin_client.get_sleep_data(day_str), day_str)),
        ("heart rate", lambda: build_heart_rate_points(garmin_client.get_heart_rates(day_str), day_str)),
        ("body composition", lambda: build_body_composition_points(garmin_client.get_body_composition(day_str), day_str)),
        ("respiration", lambda: build_respiration_points(garmin_client.get_respiration_data(day_str), day_str)),
        ("SpO2", lambda: build_spo2_points(garmin_client.get_spo2_data(day_str), day_str)),
        ("stress", lambda: build_stress_points(garmin_client.get_stress_data(day_str), day_str)),
        ("HRV", lambda: build_hrv_points(garmin_client.get_hrv_data(day_str), day_str)),
        ("hydration", lambda: build_hydration_points(garmin_client.get_hydration_data(day_str), day_str)),
        ("training readiness", lambda: build_training_readiness_points(garmin_client.get_training_readiness(day_str), day_str)),
        ("training status", lambda: build_training_status_points(garmin_client.get_training_status(day_str), day_str)),
        ("max metrics", lambda: build_max_metrics_points(garmin_client.get_max_metrics(day_str), day_str)),
        ("endurance score", lambda: build_endurance_score_points(garmin_client.get_endurance_score(day_str), day_str)),
        ("hill score", lambda: build_hill_score_points(garmin_client.get_hill_score(day_str), day_str)),
        ("fitness age", lambda: build_fitnessage_points(garmin_client.get_fitnessage_data(day_str), day_str)),
        ("floors", lambda: build_floors_points(garmin_client.get_floors(day_str), day_str)),
    ]

    for name, collect in collectors:
        try:
            pts = collect()
            if pts:
                influx_write_api.write(bucket=bucket, org=org, record=pts)
                total += len(pts)
                counts[name] = counts.get(name, 0) + len(pts)
                log.info("  %s: wrote %d points", name, len(pts))
            else:
                log.info("  %s: no data", name)
        except Exception as exc:
            if _is_no_data_not_found(exc):
                log.info("  %s: no data (not found)", name)
            else:
                log.warning("  %s: error — %s", name, exc)
                errors.append((name, exc))

    # Activities are not date-range specific; get recent ones.
    try:
        activities = garmin_client.get_activities_by_date(day_str, day_str)
        pts = build_activity_points(activities)
        if pts:
            influx_write_api.write(bucket=bucket, org=org, record=pts)
            total += len(pts)
            counts["activities"] = counts.get("activities", 0) + len(pts)
            log.info("  activities: wrote %d points", len(pts))
        else:
            log.info("  activities: no data")

        # Fetch detailed data for each activity.
        for act in (activities or []):
            act_id = act.get("activityId")
            if not act_id:
                continue
            ts_str = act.get("startTimeLocal") or act.get("startTimeGMT")
            if not ts_str:
                continue
            try:
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
            act_type = act.get("activityType", {}).get("typeKey", "unknown")
            act_name = act.get("activityName", "")

            detail_collectors = [
                ("activity details", lambda: build_activity_detail_points(
                    garmin_client.get_activity(str(act_id)),
                    act_id, act_type, act_name, ts)),
                ("activity splits", lambda: build_activity_split_points(
                    garmin_client.get_activity_splits(str(act_id)),
                    act_id, act_type, act_name, ts)),
                ("activity HR zones", lambda: build_activity_hr_zone_points(
                    garmin_client.get_activity_hr_in_timezones(str(act_id)),
                    act_id, act_type, act_name, ts)),
                ("activity weather", lambda: build_activity_weather_points(
                    garmin_client.get_activity_weather(str(act_id)),
                    act_id, act_type, act_name, ts)),
                ("activity track", lambda: build_activity_track_points(
                    garmin_client.get_activity_details(str(act_id), maxpoly=4000),
                    act_id, act_type, act_name, ts)),
            ]

            for dname, dcollect in detail_collectors:
                try:
                    dpts = dcollect()
                    if dpts:
                        influx_write_api.write(bucket=bucket, org=org, record=dpts)
                        total += len(dpts)
                        counts[dname] = counts.get(dname, 0) + len(dpts)
                        log.info("    %s [%s]: wrote %d points", dname, act_id, len(dpts))
                except Exception as dexc:
                    if _is_no_data_not_found(dexc):
                        log.debug("    %s [%s]: no data (not found)", dname, act_id)
                    else:
                        log.debug("    %s [%s]: error — %s", dname, act_id, dexc)

    except Exception as exc:
        if _is_no_data_not_found(exc):
            log.info("  activities: no data (not found)")
        else:
            log.warning("  activities: error — %s", exc)
            errors.append(("activities", exc))

    return total, errors, counts


def print_sync_summary(grand_counts, days, all_errors):
    """Print a TUI-style summary table of the sync results."""
    print()
    print("┌──────────────────────────────────────────┐")
    print("│           catGar Sync Summary            │")
    print("├────────────────────────┬─────────────────┤")
    print("│ Measurement            │ Points Written  │")
    print("├────────────────────────┼─────────────────┤")
    grand_total = 0
    for name in (
        "daily stats", "sleep", "heart rate", "body composition",
        "respiration", "SpO2", "stress", "HRV", "hydration",
        "training readiness", "training status", "max metrics",
        "endurance score", "hill score", "fitness age", "floors",
        "activities",
    ):
        count = grand_counts.get(name, 0)
        grand_total += count
        marker = "✓" if count > 0 else "·"
        print(f"│ {marker} {name:<20} │ {count:>15,} │")
    print("├────────────────────────┼─────────────────┤")
    print(f"│ {'Total':<22} │ {grand_total:>15,} │")
    print(f"│ {'Days synced':<22} │ {days:>15,} │")
    if all_errors:
        print(f"│ {'Errors':<22} │ {len(all_errors):>15,} │")
    print("└────────────────────────┴─────────────────┘")
    print()


def get_data_catalog():
    """Return a structured catalog of all data categories persisted by catGar.

    Each entry is a dict with:
        - measurement: InfluxDB measurement name
        - display_name: human-readable label used in sync summaries
        - description: what the data represents
        - garmin_api: Garmin Connect API method used to fetch the data
        - frequency: how often data is recorded (daily, per-reading, per-activity)
        - fields: list of dicts with 'garmin_key', 'influx_field', and 'description'
        - tags: list of tag names applied to the measurement (if any)
        - notes: additional context for dashboard or analysis use
    """
    return [
        {
            "measurement": "daily_stats",
            "display_name": "daily stats",
            "description": "Daily summary of steps, calories, heart rate, stress, body battery, and activity intensity.",
            "garmin_api": "get_stats(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "totalSteps", "influx_field": "steps", "description": "Total steps for the day"},
                {"garmin_key": "totalDistanceMeters", "influx_field": "distance_meters", "description": "Total distance walked/run in meters"},
                {"garmin_key": "activeKilocalories", "influx_field": "active_kcal", "description": "Calories burned through activity"},
                {"garmin_key": "totalKilocalories", "influx_field": "total_kcal", "description": "Total calories burned (active + resting)"},
                {"garmin_key": "restingHeartRate", "influx_field": "resting_hr", "description": "Resting heart rate (bpm)"},
                {"garmin_key": "maxHeartRate", "influx_field": "max_hr", "description": "Maximum heart rate recorded (bpm)"},
                {"garmin_key": "minHeartRate", "influx_field": "min_hr", "description": "Minimum heart rate recorded (bpm)"},
                {"garmin_key": "averageHeartRate", "influx_field": "avg_hr", "description": "Average heart rate (bpm)"},
                {"garmin_key": "moderateIntensityMinutes", "influx_field": "moderate_intensity_min", "description": "Minutes of moderate-intensity activity"},
                {"garmin_key": "vigorousIntensityMinutes", "influx_field": "vigorous_intensity_min", "description": "Minutes of vigorous-intensity activity"},
                {"garmin_key": "floorsAscended", "influx_field": "floors_ascended", "description": "Floors climbed up"},
                {"garmin_key": "floorsDescended", "influx_field": "floors_descended", "description": "Floors descended"},
                {"garmin_key": "averageStressLevel", "influx_field": "avg_stress", "description": "Average stress level (0-100)"},
                {"garmin_key": "maxStressLevel", "influx_field": "max_stress", "description": "Maximum stress level (0-100)"},
                {"garmin_key": "bodyBatteryChargedValue", "influx_field": "body_battery_charged", "description": "Body battery energy gained"},
                {"garmin_key": "bodyBatteryDrainedValue", "influx_field": "body_battery_drained", "description": "Body battery energy used"},
                {"garmin_key": "bodyBatteryHighestValue", "influx_field": "body_battery_high", "description": "Highest body battery level"},
                {"garmin_key": "bodyBatteryLowestValue", "influx_field": "body_battery_low", "description": "Lowest body battery level"},
            ],
            "tags": [],
            "notes": "Core daily wellness snapshot. Great for trend analysis over weeks/months.",
        },
        {
            "measurement": "sleep",
            "display_name": "sleep",
            "description": "Nightly sleep duration, stages, respiration, SpO2, heart rate, and sleep quality scores.",
            "garmin_api": "get_sleep_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "sleepTimeSeconds", "influx_field": "sleep_time_sec", "description": "Total sleep time in seconds"},
                {"garmin_key": "deepSleepSeconds", "influx_field": "deep_sleep_sec", "description": "Deep sleep duration in seconds"},
                {"garmin_key": "lightSleepSeconds", "influx_field": "light_sleep_sec", "description": "Light sleep duration in seconds"},
                {"garmin_key": "remSleepSeconds", "influx_field": "rem_sleep_sec", "description": "REM sleep duration in seconds"},
                {"garmin_key": "awakeSleepSeconds", "influx_field": "awake_sec", "description": "Time awake during sleep in seconds"},
                {"garmin_key": "averageSpO2Value", "influx_field": "avg_spo2", "description": "Average blood oxygen during sleep (%)"},
                {"garmin_key": "lowestSpO2Value", "influx_field": "lowest_spo2", "description": "Lowest blood oxygen during sleep (%)"},
                {"garmin_key": "averageRespirationValue", "influx_field": "avg_respiration", "description": "Average respiration rate during sleep (breaths/min)"},
                {"garmin_key": "lowestRespirationValue", "influx_field": "lowest_respiration", "description": "Lowest respiration rate during sleep"},
                {"garmin_key": "highestRespirationValue", "influx_field": "highest_respiration", "description": "Highest respiration rate during sleep"},
                {"garmin_key": "averageSpO2HRSleep", "influx_field": "avg_hr_sleep", "description": "Average heart rate during sleep (bpm)"},
                {"garmin_key": "sleepScores.overall", "influx_field": "score_overall", "description": "Overall sleep quality score"},
                {"garmin_key": "sleepScores.totalDuration", "influx_field": "score_totalDuration", "description": "Sleep duration score"},
                {"garmin_key": "sleepScores.stress", "influx_field": "score_stress", "description": "Sleep stress score"},
                {"garmin_key": "sleepScores.revitalizationScore", "influx_field": "score_revitalizationScore", "description": "Sleep revitalization score"},
            ],
            "tags": [],
            "notes": "Sleep data is nested under dailySleepDTO. Sleep scores may be raw numbers or {value: N} dicts.",
        },
        {
            "measurement": "heart_rate",
            "display_name": "heart rate",
            "description": "Continuous heart rate readings throughout the day, typically every 15-60 seconds.",
            "garmin_api": "get_heart_rates(date)",
            "frequency": "per-reading (sub-minute intervals)",
            "fields": [
                {"garmin_key": "heartRateValues[timestamp_ms, bpm]", "influx_field": "bpm", "description": "Heart rate in beats per minute"},
            ],
            "tags": [],
            "notes": "High-frequency time series. Timestamps are in milliseconds. Expect 500-2000+ points per day.",
        },
        {
            "measurement": "activity",
            "display_name": "activities",
            "description": "Individual workout/activity sessions (runs, rides, walks, etc.) with performance metrics.",
            "garmin_api": "get_activities_by_date(date, date)",
            "frequency": "per-activity",
            "fields": [
                {"garmin_key": "distance", "influx_field": "distance_meters", "description": "Total distance in meters"},
                {"garmin_key": "duration", "influx_field": "duration_sec", "description": "Activity duration in seconds"},
                {"garmin_key": "elapsedDuration", "influx_field": "elapsed_sec", "description": "Total elapsed time in seconds"},
                {"garmin_key": "movingDuration", "influx_field": "moving_sec", "description": "Moving time in seconds"},
                {"garmin_key": "averageHR", "influx_field": "avg_hr", "description": "Average heart rate (bpm)"},
                {"garmin_key": "maxHR", "influx_field": "max_hr", "description": "Maximum heart rate (bpm)"},
                {"garmin_key": "calories", "influx_field": "calories", "description": "Calories burned during activity"},
                {"garmin_key": "averageSpeed", "influx_field": "avg_speed", "description": "Average speed (m/s)"},
                {"garmin_key": "maxSpeed", "influx_field": "max_speed", "description": "Maximum speed (m/s)"},
                {"garmin_key": "elevationGain", "influx_field": "elevation_gain", "description": "Total elevation gained (meters)"},
                {"garmin_key": "elevationLoss", "influx_field": "elevation_loss", "description": "Total elevation lost (meters)"},
                {"garmin_key": "averageRunningCadenceInStepsPerMinute", "influx_field": "avg_cadence", "description": "Average running cadence (steps/min)"},
                {"garmin_key": "steps", "influx_field": "steps", "description": "Steps during activity"},
                {"garmin_key": "vO2MaxValue", "influx_field": "vo2max", "description": "VO2 max estimate from activity"},
                {"garmin_key": "avgPower", "influx_field": "avg_power", "description": "Average power output (watts)"},
                {"garmin_key": "maxPower", "influx_field": "max_power", "description": "Maximum power output (watts)"},
            ],
            "tags": ["type", "name"],
            "notes": "Tagged by activity type (running, cycling, etc.) and activity name. Zero or more per day.",
        },
        {
            "measurement": "activity_detail",
            "display_name": "activity details",
            "description": "Enriched per-activity metrics: training effect, performance condition, running dynamics, and power metrics.",
            "garmin_api": "get_activity(activity_id)",
            "frequency": "per-activity",
            "fields": [
                {"garmin_key": "trainingEffect", "influx_field": "training_effect_aerobic", "description": "Aerobic training effect (0-5)"},
                {"garmin_key": "anaerobicTrainingEffect", "influx_field": "training_effect_anaerobic", "description": "Anaerobic training effect (0-5)"},
                {"garmin_key": "performanceCondition", "influx_field": "performance_condition", "description": "Real-time performance condition indicator"},
                {"garmin_key": "lactateThreshold", "influx_field": "lactate_threshold", "description": "Estimated lactate threshold HR"},
                {"garmin_key": "normalizedPower", "influx_field": "normalized_power", "description": "Normalized power (watts)"},
                {"garmin_key": "groundContactTime", "influx_field": "ground_contact_time", "description": "Ground contact time (ms)"},
                {"garmin_key": "strideLength", "influx_field": "stride_length", "description": "Average stride length (meters)"},
                {"garmin_key": "verticalOscillation", "influx_field": "vertical_oscillation", "description": "Vertical oscillation (cm)"},
                {"garmin_key": "verticalRatio", "influx_field": "vertical_ratio", "description": "Vertical ratio (%)"},
                {"garmin_key": "trainingStressScore", "influx_field": "training_stress_score", "description": "Training Stress Score (TSS)"},
                {"garmin_key": "intensityFactor", "influx_field": "intensity_factor", "description": "Intensity Factor (IF)"},
                {"garmin_key": "minTemperature", "influx_field": "min_temperature", "description": "Minimum temperature during activity (°C)"},
                {"garmin_key": "maxTemperature", "influx_field": "max_temperature", "description": "Maximum temperature during activity (°C)"},
                {"garmin_key": "minElevation", "influx_field": "min_elevation", "description": "Minimum elevation (meters)"},
                {"garmin_key": "maxElevation", "influx_field": "max_elevation", "description": "Maximum elevation (meters)"},
                {"garmin_key": "lapCount", "influx_field": "lap_count", "description": "Number of laps/splits"},
            ],
            "tags": ["type", "name", "activity_id"],
            "notes": "Fetched per-activity via get_activity(). Contains advanced running dynamics and training metrics.",
        },
        {
            "measurement": "activity_split",
            "display_name": "activity splits",
            "description": "Per-split/lap breakdown of each activity with distance, pace, HR, elevation, and cadence.",
            "garmin_api": "get_activity_splits(activity_id)",
            "frequency": "per-split (multiple per activity)",
            "fields": [
                {"garmin_key": "distance", "influx_field": "distance_meters", "description": "Split distance in meters"},
                {"garmin_key": "duration", "influx_field": "duration_sec", "description": "Split duration in seconds"},
                {"garmin_key": "movingDuration", "influx_field": "moving_sec", "description": "Moving time in seconds"},
                {"garmin_key": "averageHR", "influx_field": "avg_hr", "description": "Average heart rate (bpm)"},
                {"garmin_key": "maxHR", "influx_field": "max_hr", "description": "Maximum heart rate (bpm)"},
                {"garmin_key": "averageSpeed", "influx_field": "avg_speed", "description": "Average speed (m/s)"},
                {"garmin_key": "maxSpeed", "influx_field": "max_speed", "description": "Maximum speed (m/s)"},
                {"garmin_key": "calories", "influx_field": "calories", "description": "Calories burned in split"},
                {"garmin_key": "elevationGain", "influx_field": "elevation_gain", "description": "Elevation gained in split (meters)"},
                {"garmin_key": "elevationLoss", "influx_field": "elevation_loss", "description": "Elevation lost in split (meters)"},
                {"garmin_key": "averageRunCadence", "influx_field": "avg_cadence", "description": "Average cadence (steps/min)"},
                {"garmin_key": "startLatitude", "influx_field": "start_lat", "description": "Split start latitude"},
                {"garmin_key": "startLongitude", "influx_field": "start_lon", "description": "Split start longitude"},
                {"garmin_key": "endLatitude", "influx_field": "end_lat", "description": "Split end latitude"},
                {"garmin_key": "endLongitude", "influx_field": "end_lon", "description": "Split end longitude"},
            ],
            "tags": ["type", "name", "activity_id", "split_num"],
            "notes": "GPS coordinates per split enable map reconstruction. Tagged by split number within the activity.",
        },
        {
            "measurement": "activity_hr_zone",
            "display_name": "activity HR zones",
            "description": "Time spent in each heart-rate zone during an activity.",
            "garmin_api": "get_activity_hr_in_timezones(activity_id)",
            "frequency": "per-zone (typically 5 zones per activity)",
            "fields": [
                {"garmin_key": "secsInZone", "influx_field": "secs_in_zone", "description": "Seconds spent in this HR zone"},
                {"garmin_key": "zoneLowBoundary", "influx_field": "zone_low_bpm", "description": "Zone lower boundary (bpm)"},
                {"garmin_key": "zoneHighBoundary", "influx_field": "zone_high_bpm", "description": "Zone upper boundary (bpm)"},
            ],
            "tags": ["type", "name", "activity_id", "zone"],
            "notes": "Typically 5 HR zones per activity. Useful for training intensity analysis.",
        },
        {
            "measurement": "activity_weather",
            "display_name": "activity weather",
            "description": "Weather conditions (temperature, humidity, wind) during an activity.",
            "garmin_api": "get_activity_weather(activity_id)",
            "frequency": "per-activity",
            "fields": [
                {"garmin_key": "temperature", "influx_field": "temperature_c", "description": "Temperature (°C)"},
                {"garmin_key": "apparentTemperature", "influx_field": "feels_like_c", "description": "Apparent/feels-like temperature (°C)"},
                {"garmin_key": "dewPoint", "influx_field": "dew_point_c", "description": "Dew point temperature (°C)"},
                {"garmin_key": "relativeHumidity", "influx_field": "humidity_pct", "description": "Relative humidity (%)"},
                {"garmin_key": "windDirection", "influx_field": "wind_direction_deg", "description": "Wind direction (degrees)"},
                {"garmin_key": "windSpeed", "influx_field": "wind_speed_mps", "description": "Wind speed (m/s)"},
                {"garmin_key": "windGust", "influx_field": "wind_gust_mps", "description": "Wind gust speed (m/s)"},
            ],
            "tags": ["type", "name", "activity_id"],
            "notes": "Correlate weather with performance. Not available for indoor activities.",
        },
        {
            "measurement": "activity_track",
            "display_name": "activity track",
            "description": "High-resolution GPS track points extracted from activity details, enabling full route reconstruction.",
            "garmin_api": "get_activity_details(activity_id, maxpoly=4000)",
            "frequency": "per-point (hundreds to thousands per activity)",
            "fields": [
                {"garmin_key": "directLatitude", "influx_field": "lat", "description": "Latitude in decimal degrees"},
                {"garmin_key": "directLongitude", "influx_field": "lon", "description": "Longitude in decimal degrees"},
            ],
            "tags": ["type", "name", "activity_id", "point_idx"],
            "notes": "Full-resolution GPS track. Additional per-point metrics (HR, speed, elevation, cadence, etc.) are auto-captured when available.",
        },
        {
            "measurement": "body_composition",
            "display_name": "body composition",
            "description": "Body weight, BMI, body fat percentage, muscle mass, and related metrics from a smart scale.",
            "garmin_api": "get_body_composition(date)",
            "frequency": "daily (when measured)",
            "fields": [
                {"garmin_key": "weight", "influx_field": "weight_grams", "description": "Body weight in grams"},
                {"garmin_key": "bmi", "influx_field": "bmi", "description": "Body Mass Index"},
                {"garmin_key": "bodyFat", "influx_field": "body_fat_pct", "description": "Body fat percentage"},
                {"garmin_key": "bodyWater", "influx_field": "body_water_pct", "description": "Body water percentage"},
                {"garmin_key": "muscleMass", "influx_field": "muscle_mass_grams", "description": "Muscle mass in grams"},
                {"garmin_key": "skeletalMuscleMass", "influx_field": "skeletal_muscle_mass_grams", "description": "Skeletal muscle mass in grams"},
                {"garmin_key": "boneMass", "influx_field": "bone_mass_grams", "description": "Bone mass in grams"},
                {"garmin_key": "metabolicAge", "influx_field": "metabolic_age", "description": "Estimated metabolic age (years)"},
                {"garmin_key": "visceralFat", "influx_field": "visceral_fat", "description": "Visceral fat rating"},
                {"garmin_key": "weightChange", "influx_field": "weight_change", "description": "Weight change from previous measurement"},
                {"garmin_key": "physiqueRating", "influx_field": "physique_rating", "description": "Body physique rating"},
            ],
            "tags": [],
            "notes": "Requires a Garmin-compatible smart scale. Weight is in grams (divide by 1000 for kg).",
        },
        {
            "measurement": "respiration",
            "display_name": "respiration",
            "description": "Daily respiration rate summary (waking and sleeping averages).",
            "garmin_api": "get_respiration_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "avgWakingRespirationValue", "influx_field": "avg_waking_respiration", "description": "Average waking respiration rate (breaths/min)"},
                {"garmin_key": "highestRespirationValue", "influx_field": "highest_respiration", "description": "Highest respiration rate (breaths/min)"},
                {"garmin_key": "lowestRespirationValue", "influx_field": "lowest_respiration", "description": "Lowest respiration rate (breaths/min)"},
                {"garmin_key": "avgSleepRespirationValue", "influx_field": "avg_sleep_respiration", "description": "Average sleep respiration rate (breaths/min)"},
            ],
            "tags": [],
            "notes": "Normal adult range is 12-20 breaths/min. Useful for respiratory health tracking.",
        },
        {
            "measurement": "spo2",
            "display_name": "SpO2",
            "description": "Blood oxygen saturation (SpO2) readings — average, lowest, and latest.",
            "garmin_api": "get_spo2_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "averageSpO2", "influx_field": "averageSpO2", "description": "Average blood oxygen saturation (%)"},
                {"garmin_key": "lowestSpO2", "influx_field": "lowestSpO2", "description": "Lowest blood oxygen saturation (%)"},
                {"garmin_key": "latestSpO2", "influx_field": "latestSpO2", "description": "Most recent SpO2 reading (%)"},
            ],
            "tags": [],
            "notes": "Normal SpO2 is 95-100%. Drops below 90% may indicate health concerns.",
        },
        {
            "measurement": "stress",
            "display_name": "stress",
            "description": "Daily stress levels and duration breakdown by intensity.",
            "garmin_api": "get_stress_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "avgStressLevel", "influx_field": "avg_stress", "description": "Average stress level (0-100)"},
                {"garmin_key": "maxStressLevel", "influx_field": "max_stress", "description": "Maximum stress level (0-100)"},
                {"garmin_key": "totalStressDuration", "influx_field": "total_stress_duration", "description": "Total time under stress (seconds)"},
                {"garmin_key": "lowStressDuration", "influx_field": "low_stress_duration", "description": "Time at low stress (seconds)"},
                {"garmin_key": "mediumStressDuration", "influx_field": "medium_stress_duration", "description": "Time at medium stress (seconds)"},
                {"garmin_key": "highStressDuration", "influx_field": "high_stress_duration", "description": "Time at high stress (seconds)"},
                {"garmin_key": "totalRestStressDuration", "influx_field": "rest_stress_duration", "description": "Time at rest / no stress (seconds)"},
            ],
            "tags": [],
            "notes": "Stress is derived from heart rate variability (HRV). 0-25=rest, 26-50=low, 51-75=medium, 76-100=high.",
        },
        {
            "measurement": "hrv",
            "display_name": "HRV",
            "description": "Heart Rate Variability — weekly average, nightly values, and personal baseline range.",
            "garmin_api": "get_hrv_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "weeklyAvg", "influx_field": "weekly_avg", "description": "7-day rolling HRV average (ms)"},
                {"garmin_key": "lastNight", "influx_field": "last_night", "description": "HRV during last night's sleep (ms)"},
                {"garmin_key": "lastNightAvg", "influx_field": "last_night_avg", "description": "Average nightly HRV (ms)"},
                {"garmin_key": "lastNight5MinHigh", "influx_field": "last_night_5min_high", "description": "Highest 5-minute HRV window during sleep (ms)"},
                {"garmin_key": "baseline.lowUpper", "influx_field": "baseline_lowUpper", "description": "Upper bound of low HRV baseline (ms)"},
                {"garmin_key": "baseline.balancedLow", "influx_field": "baseline_balancedLow", "description": "Lower bound of balanced HRV range (ms)"},
                {"garmin_key": "baseline.balancedUpper", "influx_field": "baseline_balancedUpper", "description": "Upper bound of balanced HRV range (ms)"},
            ],
            "tags": [],
            "notes": "HRV data may be under 'hrvSummary' key or flat. Higher HRV generally indicates better recovery.",
        },
        {
            "measurement": "hydration",
            "display_name": "hydration",
            "description": "Daily fluid intake tracking and hydration goals.",
            "garmin_api": "get_hydration_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "valueInML", "influx_field": "intake_ml", "description": "Fluid intake in milliliters"},
                {"garmin_key": "goalInML", "influx_field": "goal_ml", "description": "Daily hydration goal in milliliters"},
                {"garmin_key": "sweatLossInML", "influx_field": "sweat_loss_ml", "description": "Estimated sweat loss in milliliters"},
            ],
            "tags": [],
            "notes": "Hydration tracking is manual entry. Sweat loss is estimated from activities.",
        },
        {
            "measurement": "training_readiness",
            "display_name": "training readiness",
            "description": "Garmin's training readiness score based on sleep, recovery, and training load.",
            "garmin_api": "get_training_readiness(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "score", "influx_field": "score", "description": "Overall training readiness score (0-100)"},
                {"garmin_key": "sleepScore", "influx_field": "sleep_score", "description": "Sleep contribution to readiness"},
                {"garmin_key": "recoveryTime", "influx_field": "recovery_time", "description": "Recommended recovery time (hours)"},
                {"garmin_key": "acuteLoad", "influx_field": "acute_load", "description": "Recent training load (acute)"},
                {"garmin_key": "hrvStatus", "influx_field": "hrv_status", "description": "HRV status score"},
                {"garmin_key": "trainingLoad", "influx_field": "training_load", "description": "Current training load value"},
            ],
            "tags": [],
            "notes": "Higher scores = more ready to train. Below 33 suggests rest is needed.",
        },
        {
            "measurement": "training_status",
            "display_name": "training status",
            "description": "Training load balance, VO2 max, and lactate threshold metrics.",
            "garmin_api": "get_training_status(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "trainingLoadBalance", "influx_field": "load_balance", "description": "Training load balance ratio"},
                {"garmin_key": "ltTimestamp", "influx_field": "lt_timestamp", "description": "Lactate threshold test timestamp"},
                {"garmin_key": "vo2MaxValue", "influx_field": "vo2max", "description": "VO2 max estimate (ml/kg/min)"},
                {"garmin_key": "loadFocus", "influx_field": "load_focus", "description": "Training load focus area"},
                {"garmin_key": "lactateThresholdHeartRate", "influx_field": "lt_heart_rate", "description": "Heart rate at lactate threshold (bpm)"},
                {"garmin_key": "lactateThresholdSpeed", "influx_field": "lt_speed", "description": "Speed at lactate threshold (m/s)"},
            ],
            "tags": [],
            "notes": "Advanced training metrics. VO2 max is a key indicator of cardiovascular fitness.",
        },
        {
            "measurement": "max_metrics",
            "display_name": "max metrics",
            "description": "VO2 max and fitness age estimates, broken down by sport type.",
            "garmin_api": "get_max_metrics(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "vo2MaxPreciseValue", "influx_field": "vo2max_precise", "description": "Precise VO2 max value (ml/kg/min)"},
                {"garmin_key": "vo2MaxValue", "influx_field": "vo2max", "description": "Rounded VO2 max value"},
                {"garmin_key": "fitnessAge", "influx_field": "fitness_age", "description": "Estimated fitness age (years)"},
            ],
            "tags": ["sport"],
            "notes": "Tagged by sport (running, cycling, etc.). May contain multiple entries per day.",
        },
        {
            "measurement": "endurance_score",
            "display_name": "endurance score",
            "description": "Garmin's endurance score reflecting aerobic endurance fitness.",
            "garmin_api": "get_endurance_score(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "overallScore", "influx_field": "overall_score", "description": "Overall endurance score"},
                {"garmin_key": "enduranceScore", "influx_field": "endurance_score", "description": "Specific endurance component score"},
            ],
            "tags": [],
            "notes": "Builds over time with consistent aerobic training. Higher is better.",
        },
        {
            "measurement": "hill_score",
            "display_name": "hill score",
            "description": "Garmin's hill score reflecting climbing/uphill fitness.",
            "garmin_api": "get_hill_score(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "overallScore", "influx_field": "overall_score", "description": "Overall hill fitness score"},
                {"garmin_key": "hillScore", "influx_field": "hill_score", "description": "Specific hill component score"},
            ],
            "tags": [],
            "notes": "Improves with elevation-heavy workouts. Useful for trail runners and hikers.",
        },
        {
            "measurement": "fitness_age",
            "display_name": "fitness age",
            "description": "Estimated fitness age compared to chronological age, with contributing metrics.",
            "garmin_api": "get_fitnessage_data(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "fitnessAge", "influx_field": "fitness_age", "description": "Estimated fitness age (years)"},
                {"garmin_key": "chronologicalAge", "influx_field": "chronological_age", "description": "Actual age (years)"},
                {"garmin_key": "bmi", "influx_field": "bmi", "description": "Body Mass Index"},
                {"garmin_key": "healthyBmiTop", "influx_field": "healthy_bmi_top", "description": "Upper bound of healthy BMI range"},
                {"garmin_key": "healthyBmiBottom", "influx_field": "healthy_bmi_bottom", "description": "Lower bound of healthy BMI range"},
                {"garmin_key": "vigorousMinutes", "influx_field": "vigorous_minutes", "description": "Weekly vigorous activity minutes"},
                {"garmin_key": "vigorousMinutesGoal", "influx_field": "vigorous_minutes_goal", "description": "Weekly vigorous minutes goal"},
                {"garmin_key": "restingHr", "influx_field": "resting_hr", "description": "Resting heart rate (bpm)"},
                {"garmin_key": "restingHrGoal", "influx_field": "resting_hr_goal", "description": "Resting heart rate goal (bpm)"},
            ],
            "tags": [],
            "notes": "A fitness age lower than chronological age indicates above-average fitness for your age.",
        },
        {
            "measurement": "floors",
            "display_name": "floors",
            "description": "Daily floors (flights of stairs) climbed and descended.",
            "garmin_api": "get_floors(date)",
            "frequency": "daily",
            "fields": [
                {"garmin_key": "floorsAscended", "influx_field": "floors_ascended", "description": "Floors climbed up"},
                {"garmin_key": "floorsDescended", "influx_field": "floors_descended", "description": "Floors descended"},
                {"garmin_key": "floorsAscendedGoal", "influx_field": "floors_ascended_goal", "description": "Daily floors goal"},
            ],
            "tags": [],
            "notes": "One floor ≈ 3 meters (10 feet) of elevation gain.",
        },
    ]


def print_data_catalog():
    """Print a detailed, AI-prompt-friendly catalog of all catGar data categories.

    Designed for use as context in AI prompts, dashboard planning, and data exploration.
    Output is structured plain text that is easy to parse and reference.
    """
    catalog = get_data_catalog()

    print()
    print("=" * 80)
    print("  catGar Data Catalog")
    print("  Complete reference of all Garmin health & fitness data persisted to InfluxDB")
    print("=" * 80)
    print()
    print(f"Total data categories: {len(catalog)}")
    print(f"Total tracked fields:  {sum(len(c['fields']) for c in catalog)}")
    print()

    # Quick-reference table
    print("─" * 80)
    print("  QUICK REFERENCE")
    print("─" * 80)
    print(f"  {'Measurement':<25} {'Frequency':<30} {'Fields':>6}  {'Tags'}")
    print(f"  {'─' * 25} {'─' * 30} {'─' * 6}  {'─' * 15}")
    for cat in catalog:
        tags = ", ".join(cat["tags"]) if cat["tags"] else "—"
        print(f"  {cat['measurement']:<25} {cat['frequency']:<30} {len(cat['fields']):>6}  {tags}")
    print()

    # Detailed breakdown
    print("─" * 80)
    print("  DETAILED FIELD REFERENCE")
    print("─" * 80)

    for i, cat in enumerate(catalog, 1):
        print()
        print(f"  [{i}/{len(catalog)}] {cat['display_name'].upper()}")
        print(f"  Measurement:  {cat['measurement']}")
        print(f"  Description:  {cat['description']}")
        print(f"  Garmin API:   {cat['garmin_api']}")
        print(f"  Frequency:    {cat['frequency']}")
        if cat["tags"]:
            print(f"  Tags:         {', '.join(cat['tags'])}")
        print(f"  Notes:        {cat['notes']}")
        print()
        print(f"    {'Garmin Key':<45} {'InfluxDB Field':<30} Description")
        print(f"    {'─' * 45} {'─' * 30} {'─' * 40}")
        for f in cat["fields"]:
            print(f"    {f['garmin_key']:<45} {f['influx_field']:<30} {f['description']}")
        print()

    # Footer with usage hints
    print("─" * 80)
    print("  USAGE NOTES")
    print("─" * 80)
    print()
    print("  • All daily measurements use second-precision timestamps at midnight (00:00:00).")
    print("  • Heart rate uses millisecond-precision timestamps from the watch.")
    print("  • Activities are timestamped at their start time with second precision.")
    print("  • Additional numeric fields from Garmin are auto-discovered and stored.")
    print("  • All numeric values are stored as floats in InfluxDB.")
    print("  • Data is stored in the InfluxDB bucket configured via INFLUXDB_BUCKET (default: 'garmin').")
    print()
    print("  Example InfluxDB queries:")
    print("    from(bucket: \"garmin\") |> range(start: -7d) |> filter(fn: (r) => r._measurement == \"daily_stats\")")
    print("    from(bucket: \"garmin\") |> range(start: -30d) |> filter(fn: (r) => r._measurement == \"sleep\" and r._field == \"sleep_time_sec\")")
    print("    from(bucket: \"garmin\") |> range(start: -30d) |> filter(fn: (r) => r._measurement == \"activity\" and r.type == \"running\")")
    print()
    print("=" * 80)
    print()


def query_data_summary(influx_client, bucket, catalog_days):
    """Query InfluxDB for actual data presence and statistics per measurement.

    Returns a dict keyed by measurement name with:
        - days_with_data: number of distinct days containing data
        - total_points: total point count
        - fields: dict mapping field name to list of float values
        - tag_values: dict mapping tag name to Counter of values
    """
    query_api = influx_client.query_api()
    catalog = get_data_catalog()
    summary = {}

    for cat in catalog:
        meas = cat["measurement"]
        entry = {
            "days_with_data": 0,
            "total_points": 0,
            "fields": {},
            "tag_values": {},
        }

        # Query field values for the measurement over the last N days
        for f in cat["fields"]:
            field_name = f["influx_field"]
            query = (
                f'from(bucket: "{bucket}")'
                f" |> range(start: -{catalog_days}d)"
                f' |> filter(fn: (r) => r._measurement == "{meas}"'
                f' and r._field == "{field_name}")'
                " |> keep(columns: [\"_time\", \"_value\"])"
            )
            try:
                tables = query_api.query(query)
                values = []
                for table in tables:
                    for record in table.records:
                        v = record.get_value()
                        if v is not None:
                            try:
                                values.append(float(v))
                            except (ValueError, TypeError):
                                pass
                if values:
                    entry["fields"][field_name] = values
            except Exception as exc:
                log.debug("Query error for %s.%s: %s", meas, field_name, exc)

        # Compute days_with_data and total_points from a count query
        count_query = (
            f'from(bucket: "{bucket}")'
            f" |> range(start: -{catalog_days}d)"
            f' |> filter(fn: (r) => r._measurement == "{meas}")'
            " |> group()"
            ' |> count(column: "_value")'
        )
        try:
            tables = query_api.query(count_query)
            for table in tables:
                for record in table.records:
                    entry["total_points"] = int(record.get_value() or 0)
        except Exception:
            pass

        # Count distinct days
        days_query = (
            f'from(bucket: "{bucket}")'
            f" |> range(start: -{catalog_days}d)"
            f' |> filter(fn: (r) => r._measurement == "{meas}")'
            ' |> map(fn: (r) => ({r with _day: date.truncate(t: r._time, unit: 1d)}))'
            " |> group()"
            ' |> unique(column: "_day")'
            ' |> count(column: "_day")'
        )
        try:
            tables = query_api.query(days_query)
            for table in tables:
                for record in table.records:
                    entry["days_with_data"] = int(record.get_value() or 0)
        except Exception:
            pass

        # Query tag distributions for tagged measurements
        for tag_name in cat.get("tags", []):
            tag_query = (
                f'import "influxdata/influxdb/schema"'
                f'\nschema.tagValues(bucket: "{bucket}",'
                f' tag: "{tag_name}",'
                f" start: -{catalog_days}d,"
                f' predicate: (r) => r._measurement == "{meas}")'
            )
            try:
                tables = query_api.query(tag_query)
                tag_counter = Counter()
                for table in tables:
                    for record in table.records:
                        tag_val = record.get_value()
                        if tag_val:
                            tag_counter[str(tag_val)] = 1  # presence
                # Get actual counts per tag value
                for tag_val in tag_counter:
                    val_count_query = (
                        f'from(bucket: "{bucket}")'
                        f" |> range(start: -{catalog_days}d)"
                        f' |> filter(fn: (r) => r._measurement == "{meas}"'
                        f' and r.{tag_name} == "{tag_val}")'
                        " |> group()"
                        ' |> count(column: "_value")'
                    )
                    try:
                        val_tables = query_api.query(val_count_query)
                        for vt in val_tables:
                            for vr in vt.records:
                                tag_counter[tag_val] = int(vr.get_value() or 0)
                    except Exception:
                        pass
                if tag_counter:
                    entry["tag_values"][tag_name] = tag_counter
            except Exception as exc:
                log.debug("Tag query error for %s.%s: %s", meas, tag_name, exc)

        summary[meas] = entry

    return summary


def compute_field_stats(values):
    """Compute summary statistics for a list of numeric values.

    Returns a dict with mean, median, min, max, stdev, and count.
    """
    if not values:
        return None
    n = len(values)
    result = {
        "count": n,
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
    }
    if n >= 2:
        result["stdev"] = statistics.stdev(values)
    else:
        result["stdev"] = 0.0
    return result


def _format_stat_value(val):
    """Format a numeric value for display: ints as ints, floats to 1 decimal.

    Whole numbers below 1 million are shown without decimals for readability.
    """
    if val == int(val) and abs(val) < 1_000_000:
        return str(int(val))
    return f"{val:.1f}"


def _build_histogram(values, bins=10, width=30):
    """Build a compact horizontal ASCII histogram and return lines.

    Returns a list of strings, one per bin.
    """
    if not values:
        return []
    lo, hi = min(values), max(values)
    if lo == hi:
        return [f"  [{_format_stat_value(lo)}] {'█' * width} ({len(values)})"]
    step = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        idx = min(int((v - lo) / step), bins - 1)
        counts[idx] += 1
    max_count = max(counts) if counts else 1
    lines = []
    for i, c in enumerate(counts):
        bin_lo = lo + i * step
        bin_hi = lo + (i + 1) * step
        bar_len = int(c / max_count * width) if max_count > 0 else 0
        bar = "█" * bar_len
        label = f"{_format_stat_value(bin_lo):>8}-{_format_stat_value(bin_hi):<8}"
        lines.append(f"  {label} {bar} {c}")
    return lines


def print_data_summary(summary, catalog_days):
    """Print a TUI-style summary of actual data in the database.

    *summary* is the dict returned by ``query_data_summary()``.
    """
    catalog = get_data_catalog()
    total_fields_with_data = 0
    total_measurements_with_data = 0

    print()
    print("═" * 80)
    print("  catGar Data Summary")
    print(f"  Actual data in InfluxDB — last {catalog_days} days")
    print("═" * 80)
    print()

    # Overview table
    print("─" * 80)
    print("  DATA AVAILABILITY")
    print("─" * 80)
    print(f"  {'Measurement':<25} {'Days w/Data':>11}  {'Points':>10}  {'Fields w/Data':>13}")
    print(f"  {'─' * 25} {'─' * 11}  {'─' * 10}  {'─' * 13}")

    for cat in catalog:
        meas = cat["measurement"]
        entry = summary.get(meas, {})
        days_data = entry.get("days_with_data", 0)
        total_pts = entry.get("total_points", 0)
        fields_data = len(entry.get("fields", {}))
        total_fields_with_data += fields_data
        if days_data > 0:
            total_measurements_with_data += 1
        marker = "✓" if days_data > 0 else "·"
        print(f"  {marker} {meas:<23} {days_data:>11}  {total_pts:>10,}  {fields_data:>13}")

    print()
    print(f"  Measurements with data: {total_measurements_with_data}/{len(catalog)}")
    print(f"  Fields with data:       {total_fields_with_data}")
    print()

    # Detailed statistics per measurement
    print("─" * 80)
    print("  FIELD STATISTICS")
    print("─" * 80)

    for cat in catalog:
        meas = cat["measurement"]
        entry = summary.get(meas, {})
        fields = entry.get("fields", {})
        if not fields:
            continue

        print()
        print(f"  ▸ {cat['display_name'].upper()} ({meas})")
        print(f"    {'Field':<30} {'Count':>6} {'Min':>10} {'Mean':>10} {'Median':>10} {'Max':>10} {'StDev':>10}")
        print(f"    {'─' * 30} {'─' * 6} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")

        for f in cat["fields"]:
            fname = f["influx_field"]
            vals = fields.get(fname)
            if not vals:
                continue
            st = compute_field_stats(vals)
            print(
                f"    {fname:<30} {st['count']:>6}"
                f" {_format_stat_value(st['min']):>10}"
                f" {_format_stat_value(st['mean']):>10}"
                f" {_format_stat_value(st['median']):>10}"
                f" {_format_stat_value(st['max']):>10}"
                f" {_format_stat_value(st['stdev']):>10}"
            )

        # Show tag distributions if present
        tag_values = entry.get("tag_values", {})
        for tag_name, counter in tag_values.items():
            if counter:
                print()
                print(f"    Tag: {tag_name}")
                for val, cnt in counter.most_common(10):
                    print(f"      {val:<30} {cnt:>6} points")

    # Distribution section — show histograms for key daily metrics
    print()
    print("─" * 80)
    print("  DISTRIBUTIONS (key metrics)")
    print("─" * 80)

    # Pick interesting fields to show distributions for
    _distribution_fields = [
        ("daily_stats", "steps", "Daily Steps"),
        ("daily_stats", "resting_hr", "Resting Heart Rate"),
        ("daily_stats", "avg_stress", "Average Stress"),
        ("sleep", "sleep_time_sec", "Sleep Time (sec)"),
        ("training_readiness", "score", "Training Readiness"),
        ("body_composition", "weight_grams", "Weight (g)"),
    ]

    shown_any = False
    for meas, field, label in _distribution_fields:
        entry = summary.get(meas, {})
        vals = entry.get("fields", {}).get(field)
        if not vals or len(vals) < 2:
            continue
        shown_any = True
        st = compute_field_stats(vals)
        print()
        print(f"  {label}  (n={st['count']}, mean={_format_stat_value(st['mean'])}, median={_format_stat_value(st['median'])})")
        bin_count = min(10, len(vals))
        for line in _build_histogram(vals, bins=bin_count):
            print(f"  {line}")

    if not shown_any:
        print()
        print("  No data available for distribution charts.")

    print()
    print("═" * 80)
    print()


def main():
    parser = argparse.ArgumentParser(description="Sync Garmin data to InfluxDB")
    parser.add_argument("date", nargs="?", default=None, help="Date to sync (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--days", type=int, default=None, help="Number of past days to sync")
    parser.add_argument("--backfill", action="store_true", help="Backfill all available historical data (up to 5 years)")
    parser.add_argument("--catalog", action="store_true", help="Print detailed data catalog of all tracked categories and exit")
    parser.add_argument("--catalog-summary", action="store_true", help="Query InfluxDB and print a summary of actual data with statistics, then exit")
    parser.add_argument("--catalog-days", type=int, default=7, help="Number of days to include in --catalog-summary (default: 7)")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="Path to last-sync state file")
    args = parser.parse_args()

    if args.catalog:
        print_data_catalog()
        return

    if args.catalog_summary:
        cfg = get_config()
        influx = InfluxDBClient(
            url=cfg["INFLUXDB_URL"],
            token=cfg["INFLUXDB_TOKEN"],
            org=cfg["INFLUXDB_ORG"],
        )
        try:
            summary = query_data_summary(influx, cfg["INFLUXDB_BUCKET"], args.catalog_days)
            print_data_summary(summary, args.catalog_days)
        finally:
            influx.close()
        return

    cfg = get_config()

    # --- Garmin login ---
    log.info("Logging in to Garmin Connect…")
    garmin = Garmin(cfg["GARMIN_EMAIL"], cfg["GARMIN_PASSWORD"])
    garmin.login()
    log.info("Garmin login successful.")

    # --- InfluxDB client ---
    influx = InfluxDBClient(
        url=cfg["INFLUXDB_URL"],
        token=cfg["INFLUXDB_TOKEN"],
        org=cfg["INFLUXDB_ORG"],
    )
    write_api = influx.write_api(write_options=SYNCHRONOUS)
    ensure_bucket(influx, cfg["INFLUXDB_BUCKET"], cfg["INFLUXDB_ORG"])

    # --- Determine dates ---
    today = date.today()

    if args.date:
        # Explicit date: sync that single day
        start = datetime.strptime(args.date, "%Y-%m-%d").date()
        end = start
    elif args.backfill:
        # Full backfill: binary-search for the oldest date with data
        earliest = today - timedelta(days=BACKFILL_MAX_DAYS)
        log.info("Backfill mode: searching for oldest data in %s … %s", earliest, today)
        start = find_oldest_available_date(garmin, earliest, today)
        end = today
        log.info("Backfill mode: syncing from %s to %s", start, end)
    elif args.days is not None:
        # Explicit --days flag
        start = today - timedelta(days=args.days - 1)
        end = today
    else:
        # Auto mode: sync since last sync, or just today if no state
        last = read_last_sync(args.state_file)
        if last is not None:
            start = last + timedelta(days=1)
            if start > today:
                log.info("Already synced up to %s. Nothing to do.", last)
                influx.close()
                return

            log.info("Resuming sync from %s (last sync: %s)", start, last)
        else:
            start = today
        end = today

    days = (end - start).days + 1
    grand_total = 0
    all_errors = []
    grand_counts = {}

    for i in range(days):
        day = start + timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        log.info("Syncing %s …", day_str)
        total, errors, counts = fetch_and_write(garmin, write_api, cfg["INFLUXDB_BUCKET"], cfg["INFLUXDB_ORG"], day_str)
        grand_total += total
        all_errors.extend(errors)
        for name, cnt in counts.items():
            grand_counts[name] = grand_counts.get(name, 0) + cnt

    influx.close()

    # Record last successful sync date (only if no errors)
    if not all_errors:
        write_last_sync(end, args.state_file)

    print_sync_summary(grand_counts, days, all_errors)
    log.info("Done. Wrote %d total points across %d day(s).", grand_total, days)
    if all_errors:
        log.warning("Encountered %d error(s) during sync.", len(all_errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
