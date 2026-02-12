#!/usr/bin/env python3
"""catGar — sync Garmin watch data to InfluxDB.

Usage:
    python catgar.py              # sync today's data
    python catgar.py 2024-01-15   # sync a specific date
    python catgar.py --days 7     # sync last 7 days
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from garminconnect import Garmin
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("catgar")


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


# ---------------------------------------------------------------------------
# Garmin data → InfluxDB points
# ---------------------------------------------------------------------------

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
            p = (
                Point("daily_stats")
                .time(ts, WritePrecision.S)
                .field(influx_field, float(val))
            )
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
            p = (
                Point("sleep")
                .time(ts, WritePrecision.S)
                .field(influx_field, float(val))
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
                p = (
                    Point("sleep")
                    .time(ts, WritePrecision.S)
                    .field(f"score_{score_key}", float(val))
                )
                points.append(p)

    return points


def build_heart_rate_points(hr_data, day_str):
    """Convert Garmin heart-rate data into InfluxDB Point objects."""
    points = []

    for entry in (hr_data or []):
        hr_values = entry.get("heartRateValues")
        if not hr_values:
            continue
        for ts_ms, hr in hr_values:
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
                p = p.field(influx_field, float(val))
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
        "boneMass": "bone_mass_grams",
        "metabolicAge": "metabolic_age",
        "visceralFat": "visceral_fat",
    }

    for garmin_key, influx_field in field_map.items():
        val = body_data.get(garmin_key)
        if val is not None:
            p = (
                Point("body_composition")
                .time(ts, WritePrecision.S)
                .field(influx_field, float(val))
            )
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
            p = (
                Point("respiration")
                .time(ts, WritePrecision.S)
                .field(influx_field, float(val))
            )
            points.append(p)

    return points


def build_spo2_points(spo2_data, day_str):
    """Convert Garmin SpO2 data into InfluxDB Point objects."""
    points = []
    ts = datetime.strptime(day_str, "%Y-%m-%d")

    if not spo2_data:
        return points

    for key in ("averageSpO2", "lowestSpO2", "latestSpO2"):
        val = spo2_data.get(key)
        if val is not None:
            p = (
                Point("spo2")
                .time(ts, WritePrecision.S)
                .field(key, float(val))
            )
            points.append(p)

    return points


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def fetch_and_write(garmin_client, influx_write_api, bucket, org, day_str):
    """Fetch all Garmin data for *day_str* and write to InfluxDB."""
    total = 0
    errors = []

    collectors = [
        ("daily stats", lambda: build_daily_stats_points(garmin_client.get_stats(day_str), day_str)),
        ("sleep", lambda: build_sleep_points(garmin_client.get_sleep_data(day_str), day_str)),
        ("heart rate", lambda: build_heart_rate_points(garmin_client.get_heart_rates(day_str), day_str)),
        ("body composition", lambda: build_body_composition_points(garmin_client.get_body_composition(day_str), day_str)),
        ("respiration", lambda: build_respiration_points(garmin_client.get_respiration_data(day_str), day_str)),
        ("SpO2", lambda: build_spo2_points(garmin_client.get_spo2_data(day_str), day_str)),
    ]

    for name, collect in collectors:
        try:
            pts = collect()
            if pts:
                influx_write_api.write(bucket=bucket, org=org, record=pts)
                total += len(pts)
                log.info("  %s: wrote %d points", name, len(pts))
            else:
                log.info("  %s: no data", name)
        except Exception as exc:
            log.warning("  %s: error — %s", name, exc)
            errors.append((name, exc))

    # Activities are not date-range specific; get recent ones.
    try:
        activities = garmin_client.get_activities_by_date(day_str, day_str)
        pts = build_activity_points(activities)
        if pts:
            influx_write_api.write(bucket=bucket, org=org, record=pts)
            total += len(pts)
            log.info("  activities: wrote %d points", len(pts))
        else:
            log.info("  activities: no data")
    except Exception as exc:
        log.warning("  activities: error — %s", exc)
        errors.append(("activities", exc))

    return total, errors


def main():
    parser = argparse.ArgumentParser(description="Sync Garmin data to InfluxDB")
    parser.add_argument("date", nargs="?", default=None, help="Date to sync (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--days", type=int, default=1, help="Number of past days to sync (default: 1 = today only)")
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

    # --- Determine dates ---
    if args.date:
        start = datetime.strptime(args.date, "%Y-%m-%d").date()
        days = 1
    else:
        days = args.days
        start = date.today() - timedelta(days=days - 1)

    grand_total = 0
    all_errors = []

    for i in range(days):
        day = start + timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        log.info("Syncing %s …", day_str)
        total, errors = fetch_and_write(garmin, write_api, cfg["INFLUXDB_BUCKET"], cfg["INFLUXDB_ORG"], day_str)
        grand_total += total
        all_errors.extend(errors)

    influx.close()

    log.info("Done. Wrote %d total points across %d day(s).", grand_total, days)
    if all_errors:
        log.warning("Encountered %d error(s) during sync.", len(all_errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
