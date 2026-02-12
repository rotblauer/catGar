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
import sys
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
    print("│          catGar Sync Summary              │")
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


def main():
    parser = argparse.ArgumentParser(description="Sync Garmin data to InfluxDB")
    parser.add_argument("date", nargs="?", default=None, help="Date to sync (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--days", type=int, default=None, help="Number of past days to sync")
    parser.add_argument("--backfill", action="store_true", help="Backfill all available historical data (up to 5 years)")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE, help="Path to last-sync state file")
    args = parser.parse_args()

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
