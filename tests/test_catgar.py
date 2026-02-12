"""Unit tests for catgar data transformation functions."""

import io
import json
import logging
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from catgar import (
    _build_histogram,
    _collect_extra_fields,
    _format_stat_value,
    _safe_float,
    build_activity_points,
    build_body_composition_points,
    build_daily_stats_points,
    build_endurance_score_points,
    build_fitnessage_points,
    build_floors_points,
    build_heart_rate_points,
    build_hill_score_points,
    build_hrv_points,
    build_hydration_points,
    build_max_metrics_points,
    build_respiration_points,
    build_sleep_points,
    build_spo2_points,
    build_stress_points,
    build_training_readiness_points,
    build_training_status_points,
    compute_field_stats,
    ensure_bucket,
    find_oldest_available_date,
    get_data_catalog,
    print_data_catalog,
    print_data_summary,
    print_sync_summary,
    query_data_summary,
    read_last_sync,
    write_last_sync,
)


class TestBuildDailyStatsPoints(unittest.TestCase):
    def test_basic_stats(self):
        stats = {
            "totalSteps": 8500,
            "restingHeartRate": 58,
            "totalKilocalories": 2200,
        }
        pts = build_daily_stats_points(stats, "2024-06-01")
        self.assertEqual(len(pts), 3)
        # All points should use measurement "daily_stats"
        for p in pts:
            self.assertIn("daily_stats", p.to_line_protocol())

    def test_empty_stats(self):
        pts = build_daily_stats_points({}, "2024-06-01")
        self.assertEqual(pts, [])

    def test_none_values_skipped(self):
        stats = {"totalSteps": None, "restingHeartRate": 60}
        pts = build_daily_stats_points(stats, "2024-06-01")
        self.assertEqual(len(pts), 1)

    def test_timestamp_in_line_protocol(self):
        stats = {"totalSteps": 100}
        pts = build_daily_stats_points(stats, "2024-06-01")
        lp = pts[0].to_line_protocol()
        # Should contain unix timestamp for 2024-06-01 00:00:00
        expected_ts = int(datetime(2024, 6, 1).timestamp())
        self.assertIn(str(expected_ts), lp)


class TestBuildSleepPoints(unittest.TestCase):
    def test_basic_sleep(self):
        data = {
            "dailySleepDTO": {
                "sleepTimeSeconds": 28800,
                "deepSleepSeconds": 7200,
                "lightSleepSeconds": 14400,
                "remSleepSeconds": 5400,
                "awakeSleepSeconds": 1800,
            }
        }
        pts = build_sleep_points(data, "2024-06-01")
        self.assertEqual(len(pts), 5)

    def test_sleep_with_scores(self):
        data = {
            "dailySleepDTO": {
                "sleepTimeSeconds": 28800,
                "sleepScores": {
                    "overall": {"value": 82},
                    "totalDuration": {"value": 75},
                },
            }
        }
        pts = build_sleep_points(data, "2024-06-01")
        # 1 field + 2 scores = 3 points
        self.assertEqual(len(pts), 3)

    def test_empty_sleep(self):
        pts = build_sleep_points({}, "2024-06-01")
        self.assertEqual(pts, [])

    def test_sleep_scores_raw_values(self):
        """Sleep scores might be raw numbers instead of dicts."""
        data = {
            "dailySleepDTO": {
                "sleepScores": {
                    "overall": 85,
                    "revitalizationScore": 70,
                },
            }
        }
        pts = build_sleep_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)


class TestBuildHeartRatePoints(unittest.TestCase):
    def test_basic_hr(self):
        data = [
            {
                "startTimestampGMT": "2024-06-01T00:00:00.0",
                "heartRateValues": [
                    [1717200000000, 65],
                    [1717200060000, 68],
                ],
            }
        ]
        pts = build_heart_rate_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_hr_skipped(self):
        data = [
            {
                "startTimestampGMT": "2024-06-01T00:00:00.0",
                "heartRateValues": [
                    [1717200000000, None],
                    [1717200060000, 68],
                ],
            }
        ]
        pts = build_heart_rate_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)

    def test_empty_hr(self):
        pts = build_heart_rate_points([], "2024-06-01")
        self.assertEqual(pts, [])

    def test_none_input(self):
        pts = build_heart_rate_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_invalid_entries_skipped(self):
        data = [
            "oops",
            {"heartRateValues": "bad"},
            {"heartRateValues": [[1717200000000, 65], "bad", [None, 70]]},
        ]
        pts = build_heart_rate_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)


class TestBuildActivityPoints(unittest.TestCase):
    def test_basic_activity(self):
        activities = [
            {
                "startTimeLocal": "2024-06-01 07:30:00",
                "activityType": {"typeKey": "running"},
                "activityName": "Morning Run",
                "distance": 5000.0,
                "duration": 1800.0,
                "averageHR": 145.0,
                "calories": 350.0,
            }
        ]
        pts = build_activity_points(activities)
        self.assertEqual(len(pts), 1)
        lp = pts[0].to_line_protocol()
        self.assertIn("activity", lp)
        self.assertIn("type=running", lp)

    def test_empty_activities(self):
        pts = build_activity_points([])
        self.assertEqual(pts, [])

    def test_none_activities(self):
        pts = build_activity_points(None)
        self.assertEqual(pts, [])

    def test_activity_missing_time_skipped(self):
        activities = [{"distance": 5000.0}]
        pts = build_activity_points(activities)
        self.assertEqual(pts, [])

    def test_activity_no_numeric_fields_skipped(self):
        activities = [
            {
                "startTimeLocal": "2024-06-01 07:30:00",
                "activityType": {"typeKey": "running"},
                "activityName": "Morning Run",
            }
        ]
        pts = build_activity_points(activities)
        self.assertEqual(pts, [])


class TestBuildBodyCompositionPoints(unittest.TestCase):
    def test_basic_body(self):
        data = {"weight": 75000.0, "bmi": 24.5, "bodyFat": 18.0}
        pts = build_body_composition_points(data, "2024-06-01")
        self.assertEqual(len(pts), 3)

    def test_empty_body(self):
        pts = build_body_composition_points({}, "2024-06-01")
        self.assertEqual(pts, [])

    def test_none_body(self):
        pts = build_body_composition_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildRespirationPoints(unittest.TestCase):
    def test_basic_respiration(self):
        data = {
            "avgWakingRespirationValue": 16.0,
            "highestRespirationValue": 22.0,
            "lowestRespirationValue": 12.0,
        }
        pts = build_respiration_points(data, "2024-06-01")
        self.assertEqual(len(pts), 3)

    def test_none_respiration(self):
        pts = build_respiration_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildSpO2Points(unittest.TestCase):
    def test_basic_spo2(self):
        data = {"averageSpO2": 96.0, "lowestSpO2": 92.0, "latestSpO2": 97.0}
        pts = build_spo2_points(data, "2024-06-01")
        self.assertEqual(len(pts), 3)

    def test_none_spo2(self):
        pts = build_spo2_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_partial_spo2(self):
        data = {"averageSpO2": 95.0}
        pts = build_spo2_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)


class TestLastSyncState(unittest.TestCase):
    def setUp(self):
        self.tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        self.tmpfile.close()
        self.state_path = self.tmpfile.name

    def tearDown(self):
        if os.path.exists(self.state_path):
            os.unlink(self.state_path)

    def test_read_missing_file(self):
        os.unlink(self.state_path)
        result = read_last_sync(self.state_path)
        self.assertIsNone(result)

    def test_write_and_read(self):
        d = date(2024, 6, 15)
        write_last_sync(d, self.state_path)
        result = read_last_sync(self.state_path)
        self.assertEqual(result, d)

    def test_read_corrupt_file(self):
        with open(self.state_path, "w") as f:
            f.write("not json")
        result = read_last_sync(self.state_path)
        self.assertIsNone(result)

    def test_read_missing_key(self):
        with open(self.state_path, "w") as f:
            json.dump({"other": "value"}, f)
        result = read_last_sync(self.state_path)
        self.assertIsNone(result)

    def test_write_overwrites(self):
        write_last_sync(date(2024, 1, 1), self.state_path)
        write_last_sync(date(2024, 6, 15), self.state_path)
        result = read_last_sync(self.state_path)
        self.assertEqual(result, date(2024, 6, 15))


class TestSafeFloat(unittest.TestCase):
    def test_valid_int(self):
        self.assertEqual(_safe_float(42, "f", "m"), 42.0)

    def test_valid_float(self):
        self.assertEqual(_safe_float(3.14, "f", "m"), 3.14)

    def test_valid_string_number(self):
        self.assertEqual(_safe_float("99.5", "f", "m"), 99.5)

    def test_invalid_string(self):
        self.assertIsNone(_safe_float("not_a_number", "f", "m"))

    def test_dict_value(self):
        self.assertIsNone(_safe_float({"value": 1}, "f", "m"))

    def test_list_value(self):
        self.assertIsNone(_safe_float([1, 2], "f", "m"))


class TestMisparsedDataNotDropped(unittest.TestCase):
    """Ensure un-parseable values are logged, not silently dropped."""

    def test_daily_stats_bad_value_logged(self):
        stats = {"totalSteps": "bad_value", "restingHeartRate": 60}
        pts = build_daily_stats_points(stats, "2024-06-01")
        # The good value is kept; the bad one is skipped with a warning
        self.assertEqual(len(pts), 1)

    def test_sleep_bad_score_logged(self):
        data = {
            "dailySleepDTO": {
                "sleepTimeSeconds": "oops",
                "sleepScores": {"overall": "bad"},
            }
        }
        pts = build_sleep_points(data, "2024-06-01")
        self.assertEqual(len(pts), 0)

    def test_activity_bad_numeric_field_skipped(self):
        activities = [
            {
                "startTimeLocal": "2024-06-01 07:30:00",
                "activityType": {"typeKey": "running"},
                "activityName": "Run",
                "distance": "not_a_number",
                "calories": 350.0,
            }
        ]
        pts = build_activity_points(activities)
        self.assertEqual(len(pts), 1)
        lp = pts[0].to_line_protocol()
        self.assertIn("calories=350", lp)
        self.assertNotIn("distance", lp)

    def test_body_composition_bad_value(self):
        data = {"weight": "heavy", "bmi": 24.5}
        pts = build_body_composition_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)

    def test_respiration_bad_value(self):
        data = {"avgWakingRespirationValue": [], "lowestRespirationValue": 12.0}
        pts = build_respiration_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)

    def test_spo2_bad_value(self):
        data = {"averageSpO2": {}, "lowestSpO2": 92.0}
        pts = build_spo2_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)


class TestFindOldestAvailableDate(unittest.TestCase):
    """Test the binary search for oldest available Garmin data."""

    def _make_garmin(self, data_start_date):
        """Return a mock Garmin client that has data from *data_start_date* onward."""
        client = MagicMock()

        def fake_get_stats(day_str):
            d = datetime.strptime(day_str, "%Y-%m-%d").date()
            if d >= data_start_date:
                return {"totalSteps": 5000, "restingHeartRate": 60}
            return {}

        client.get_stats = fake_get_stats
        return client

    def test_finds_exact_start(self):
        data_start = date(2023, 6, 15)
        client = self._make_garmin(data_start)
        result = find_oldest_available_date(client, date(2023, 1, 1), date(2024, 1, 1))
        self.assertEqual(result, data_start)

    def test_earliest_has_data(self):
        client = self._make_garmin(date(2022, 1, 1))
        result = find_oldest_available_date(client, date(2023, 1, 1), date(2024, 1, 1))
        self.assertEqual(result, date(2023, 1, 1))

    def test_no_data_returns_latest(self):
        client = self._make_garmin(date(2099, 1, 1))  # no data in range
        result = find_oldest_available_date(client, date(2023, 1, 1), date(2024, 1, 1))
        self.assertEqual(result, date(2024, 1, 1))

    def test_same_day_range(self):
        client = self._make_garmin(date(2024, 1, 1))
        result = find_oldest_available_date(client, date(2024, 1, 1), date(2024, 1, 1))
        # earliest == latest and has data
        self.assertEqual(result, date(2024, 1, 1))

    def test_data_starts_near_end(self):
        data_start = date(2023, 12, 25)
        client = self._make_garmin(data_start)
        result = find_oldest_available_date(client, date(2023, 1, 1), date(2024, 1, 1))
        self.assertEqual(result, data_start)


class TestEnsureBucket(unittest.TestCase):
    """Test bucket creation with infinite retention."""

    def test_creates_bucket_with_infinite_retention(self):
        mock_client = MagicMock()
        mock_buckets_api = MagicMock()
        mock_client.buckets_api.return_value = mock_buckets_api
        mock_buckets_api.find_bucket_by_name.return_value = None

        ensure_bucket(mock_client, "test_bucket", "test_org")

        mock_buckets_api.create_bucket.assert_called_once()
        _, kwargs = mock_buckets_api.create_bucket.call_args
        rules = kwargs.get("retention_rules", [])
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].every_seconds, 0)

    def test_existing_bucket_no_create(self):
        mock_client = MagicMock()
        mock_buckets_api = MagicMock()
        mock_client.buckets_api.return_value = mock_buckets_api
        mock_bucket = MagicMock()
        mock_bucket.retention_rules = []
        mock_buckets_api.find_bucket_by_name.return_value = mock_bucket

        ensure_bucket(mock_client, "existing", "org")

        mock_buckets_api.create_bucket.assert_not_called()

    def test_existing_bucket_finite_retention_warns(self):
        mock_client = MagicMock()
        mock_buckets_api = MagicMock()
        mock_client.buckets_api.return_value = mock_buckets_api
        mock_rule = MagicMock()
        mock_rule.every_seconds = 86400
        mock_bucket = MagicMock()
        mock_bucket.retention_rules = [mock_rule]
        mock_buckets_api.find_bucket_by_name.return_value = mock_bucket

        with patch("catgar.log") as mock_log:
            ensure_bucket(mock_client, "my_bucket", "org")
            mock_log.warning.assert_called_once()
            self.assertIn("finite retention", mock_log.warning.call_args[0][0])


class TestCollectExtraFields(unittest.TestCase):
    """Test the _collect_extra_fields helper for graceful discovery."""

    def test_captures_unknown_numeric_fields(self):
        data = {"known": 1, "unknown_field": 42.5, "another": 10}
        extras = _collect_extra_fields(data, {"known"}, "test")
        self.assertIn("unknown_field", extras)
        self.assertEqual(extras["unknown_field"], 42.5)
        self.assertIn("another", extras)

    def test_skips_known_keys(self):
        data = {"known": 1, "unknown": 2}
        extras = _collect_extra_fields(data, {"known", "unknown"}, "test")
        self.assertEqual(extras, {})

    def test_skips_none_values(self):
        data = {"extra": None}
        extras = _collect_extra_fields(data, set(), "test")
        self.assertEqual(extras, {})

    def test_skips_dict_and_list_values(self):
        data = {"nested": {"a": 1}, "arr": [1, 2]}
        extras = _collect_extra_fields(data, set(), "test")
        self.assertEqual(extras, {})

    def test_skips_ignored_metadata_keys(self):
        data = {"calendarDate": "2024-06-01", "userProfilePK": 12345, "newField": 99}
        extras = _collect_extra_fields(data, set(), "test")
        self.assertNotIn("calendarDate", extras)
        self.assertNotIn("userProfilePK", extras)
        self.assertIn("newField", extras)

    def test_non_dict_input(self):
        extras = _collect_extra_fields("not a dict", set(), "test")
        self.assertEqual(extras, {})

    def test_string_number_coerced(self):
        data = {"strNum": "3.14"}
        extras = _collect_extra_fields(data, set(), "test")
        self.assertEqual(extras["strNum"], 3.14)

    def test_unparseable_string_skipped(self):
        data = {"badStr": "not_a_number"}
        extras = _collect_extra_fields(data, set(), "test")
        self.assertEqual(extras, {})


class TestBuildStressPoints(unittest.TestCase):
    def test_basic_stress(self):
        data = {
            "avgStressLevel": 35,
            "maxStressLevel": 75,
            "highStressDuration": 3600,
            "lowStressDuration": 7200,
        }
        pts = build_stress_points(data, "2024-06-01")
        self.assertEqual(len(pts), 4)
        for p in pts:
            self.assertIn("stress", p.to_line_protocol())

    def test_none_stress(self):
        pts = build_stress_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_empty_stress(self):
        pts = build_stress_points({}, "2024-06-01")
        self.assertEqual(pts, [])

    def test_extra_fields_captured(self):
        data = {"avgStressLevel": 35, "brandNewField": 99}
        pts = build_stress_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)
        lp = " ".join(p.to_line_protocol() for p in pts)
        self.assertIn("brandNewField", lp)


class TestBuildHrvPoints(unittest.TestCase):
    def test_basic_hrv(self):
        data = {
            "hrvSummary": {
                "weeklyAvg": 45,
                "lastNight": 42,
                "lastNightAvg": 40,
                "lastNight5MinHigh": 55,
            }
        }
        pts = build_hrv_points(data, "2024-06-01")
        self.assertEqual(len(pts), 4)

    def test_hrv_with_baseline(self):
        data = {
            "hrvSummary": {
                "weeklyAvg": 45,
                "baseline": {
                    "lowUpper": 30,
                    "balancedLow": 35,
                    "balancedUpper": 55,
                },
            }
        }
        pts = build_hrv_points(data, "2024-06-01")
        # 1 field + 3 baseline fields = 4
        self.assertEqual(len(pts), 4)

    def test_none_hrv(self):
        pts = build_hrv_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_empty_hrv(self):
        pts = build_hrv_points({}, "2024-06-01")
        self.assertEqual(pts, [])

    def test_flat_hrv_data(self):
        """HRV data without hrvSummary wrapper."""
        data = {"weeklyAvg": 45, "lastNight": 42}
        pts = build_hrv_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)


class TestBuildHydrationPoints(unittest.TestCase):
    def test_basic_hydration(self):
        data = {"valueInML": 2000, "goalInML": 2500}
        pts = build_hydration_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_hydration(self):
        pts = build_hydration_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_extra_hydration_fields(self):
        data = {"valueInML": 2000, "someNewField": 123}
        pts = build_hydration_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)


class TestBuildTrainingReadinessPoints(unittest.TestCase):
    def test_basic_readiness(self):
        data = {"score": 72, "sleepScore": 80, "recoveryTime": 24}
        pts = build_training_readiness_points(data, "2024-06-01")
        self.assertEqual(len(pts), 3)

    def test_none_readiness(self):
        pts = build_training_readiness_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildTrainingStatusPoints(unittest.TestCase):
    def test_basic_status(self):
        data = {"vo2MaxValue": 48.5, "trainingLoadBalance": 1.2}
        pts = build_training_status_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_status(self):
        pts = build_training_status_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildMaxMetricsPoints(unittest.TestCase):
    def test_basic_max_metrics_list(self):
        data = [
            {"sport": "running", "vo2MaxPreciseValue": 48.5, "vo2MaxValue": 49},
            {"sport": "cycling", "vo2MaxPreciseValue": 45.0},
        ]
        pts = build_max_metrics_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)
        lp = pts[0].to_line_protocol()
        self.assertIn("sport=running", lp)

    def test_dict_with_max_metrics_key(self):
        data = {"maxMetrics": [{"sport": "running", "vo2MaxValue": 49}]}
        pts = build_max_metrics_points(data, "2024-06-01")
        self.assertEqual(len(pts), 1)

    def test_none_max_metrics(self):
        pts = build_max_metrics_points(None, "2024-06-01")
        self.assertEqual(pts, [])

    def test_empty_list(self):
        pts = build_max_metrics_points([], "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildEnduranceScorePoints(unittest.TestCase):
    def test_basic_endurance(self):
        data = {"overallScore": 55, "enduranceScore": 60}
        pts = build_endurance_score_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_endurance(self):
        pts = build_endurance_score_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildHillScorePoints(unittest.TestCase):
    def test_basic_hill(self):
        data = {"overallScore": 45, "hillScore": 50}
        pts = build_hill_score_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_hill(self):
        pts = build_hill_score_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildFitnessagePoints(unittest.TestCase):
    def test_basic_fitnessage(self):
        data = {
            "fitnessAge": 28,
            "chronologicalAge": 35,
            "bmi": 23.5,
        }
        pts = build_fitnessage_points(data, "2024-06-01")
        self.assertEqual(len(pts), 3)

    def test_none_fitnessage(self):
        pts = build_fitnessage_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestBuildFloorsPoints(unittest.TestCase):
    def test_basic_floors(self):
        data = {"floorsAscended": 12, "floorsDescended": 10}
        pts = build_floors_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_none_floors(self):
        pts = build_floors_points(None, "2024-06-01")
        self.assertEqual(pts, [])


class TestExtraFieldsInExistingBuilders(unittest.TestCase):
    """Ensure existing builders now capture unknown numeric fields."""

    def test_daily_stats_extra_field(self):
        stats = {"totalSteps": 8500, "brandNewGarminField": 42}
        pts = build_daily_stats_points(stats, "2024-06-01")
        self.assertEqual(len(pts), 2)
        lp = " ".join(p.to_line_protocol() for p in pts)
        self.assertIn("brandNewGarminField", lp)

    def test_body_composition_extra_field(self):
        data = {"weight": 75000, "newBodyMetric": 1.5}
        pts = build_body_composition_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_body_composition_skeletal_muscle_mass(self):
        data = {"skeletalMuscleMass": 32000, "weightChange": -500}
        pts = build_body_composition_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)
        lp = " ".join(p.to_line_protocol() for p in pts)
        self.assertIn("skeletal_muscle_mass_grams", lp)
        self.assertIn("weight_change", lp)

    def test_respiration_extra_field(self):
        data = {"avgWakingRespirationValue": 16, "newRespField": 5}
        pts = build_respiration_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)

    def test_spo2_extra_field(self):
        data = {"averageSpO2": 96, "newSpo2Metric": 88}
        pts = build_spo2_points(data, "2024-06-01")
        self.assertEqual(len(pts), 2)


class TestIgnoredStringFields(unittest.TestCase):
    """Ensure known string/timestamp fields are silently skipped."""

    def test_daily_stats_ignores_timestamp_and_string_fields(self):
        """Timestamp and string fields from daily_stats should not produce warnings."""
        stats = {
            "totalSteps": 8500,
            "wellnessStartTimeGmt": "2024-08-14T05:00:00.0",
            "wellnessStartTimeLocal": "2024-08-14T00:00:00.0",
            "wellnessEndTimeGmt": "2024-08-15T05:00:00.0",
            "wellnessEndTimeLocal": "2024-08-15T00:00:00.0",
            "source": "GARMIN",
            "stressQualifier": "UNKNOWN",
            "latestSpo2ReadingTimeGmt": "2024-08-15T03:05:00.0",
            "latestSpo2ReadingTimeLocal": "2024-08-14T22:05:00.0",
            "latestRespirationTimeGMT": "2024-08-15T05:00:00.0",
        }
        with patch("catgar.log") as mock_log:
            pts = build_daily_stats_points(stats, "2024-06-01")
            # Only totalSteps should be captured
            self.assertEqual(len(pts), 1)
            mock_log.warning.assert_not_called()

    def test_body_composition_ignores_date_fields(self):
        """startDate and endDate should not produce warnings."""
        data = {
            "weight": 75000,
            "startDate": "2024-08-14",
            "endDate": "2024-08-14",
        }
        with patch("catgar.log") as mock_log:
            pts = build_body_composition_points(data, "2024-06-01")
            self.assertEqual(len(pts), 1)
            mock_log.warning.assert_not_called()

    def test_respiration_ignores_sleep_timestamp_fields(self):
        """Sleep timestamp fields in respiration data should not produce warnings."""
        data = {
            "avgWakingRespirationValue": 16.0,
            "tomorrowSleepStartTimestampGMT": "2024-08-15T04:07:00.0",
            "tomorrowSleepEndTimestampGMT": "2024-08-15T14:24:00.0",
            "tomorrowSleepStartTimestampLocal": "2024-08-14T23:07:00.0",
            "tomorrowSleepEndTimestampLocal": "2024-08-15T09:24:00.0",
        }
        with patch("catgar.log") as mock_log:
            pts = build_respiration_points(data, "2024-06-01")
            self.assertEqual(len(pts), 1)
            mock_log.warning.assert_not_called()

    def test_spo2_ignores_timestamp_fields(self):
        """Timestamp fields in SpO2 data should not produce warnings."""
        data = {
            "averageSpO2": 96.0,
            "tomorrowSleepStartTimestampGMT": "2024-08-15T04:07:00.0",
            "tomorrowSleepEndTimestampGMT": "2024-08-15T14:24:00.0",
            "tomorrowSleepStartTimestampLocal": "2024-08-14T23:07:00.0",
            "tomorrowSleepEndTimestampLocal": "2024-08-15T09:24:00.0",
            "latestSpO2TimestampGMT": "2024-08-15T03:05:00.0",
            "latestSpO2TimestampLocal": "2024-08-14T22:05:00.0",
        }
        with patch("catgar.log") as mock_log:
            pts = build_spo2_points(data, "2024-06-01")
            self.assertEqual(len(pts), 1)
            mock_log.warning.assert_not_called()

    def test_collect_extra_fields_skips_new_ignored_keys(self):
        """_collect_extra_fields should silently skip all ignored keys."""
        data = {
            "numericField": 42,
            "wellnessStartTimeGmt": "2024-08-14T05:00:00.0",
            "source": "GARMIN",
            "startDate": "2024-08-14",
            "tomorrowSleepStartTimestampGMT": "2024-08-15T04:07:00.0",
            "latestSpO2TimestampGMT": "2024-08-15T03:05:00.0",
        }
        extras = _collect_extra_fields(data, set(), "test")
        self.assertEqual(list(extras.keys()), ["numericField"])


class TestPrintSyncSummary(unittest.TestCase):
    """Test the TUI summary output."""

    def test_summary_with_data(self):
        counts = {"daily stats": 67, "sleep": 5, "heart rate": 1440}
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_sync_summary(counts, 3, [])
            output = mock_out.getvalue()
        self.assertIn("catGar Sync Summary", output)
        self.assertIn("daily stats", output)
        self.assertIn("67", output)
        self.assertIn("1,440", output)
        self.assertIn("Days synced", output)
        self.assertIn("3", output)

    def test_summary_with_errors(self):
        counts = {"daily stats": 10}
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_sync_summary(counts, 1, [("sleep", Exception("fail"))])
            output = mock_out.getvalue()
        self.assertIn("Errors", output)

    def test_summary_empty_sync(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_sync_summary({}, 1, [])
            output = mock_out.getvalue()
        self.assertIn("catGar Sync Summary", output)
        self.assertIn("Total", output)


class TestGetDataCatalog(unittest.TestCase):
    """Test the data catalog functions."""

    def test_catalog_returns_all_categories(self):
        catalog = get_data_catalog()
        self.assertEqual(len(catalog), 17)

    def test_catalog_entry_structure(self):
        catalog = get_data_catalog()
        required_keys = {"measurement", "display_name", "description", "garmin_api",
                         "frequency", "fields", "tags", "notes"}
        for entry in catalog:
            self.assertEqual(set(entry.keys()), required_keys)

    def test_catalog_field_structure(self):
        catalog = get_data_catalog()
        field_keys = {"garmin_key", "influx_field", "description"}
        for entry in catalog:
            self.assertGreater(len(entry["fields"]), 0, f"{entry['measurement']} has no fields")
            for f in entry["fields"]:
                self.assertEqual(set(f.keys()), field_keys)

    def test_catalog_measurement_names_unique(self):
        catalog = get_data_catalog()
        measurements = [c["measurement"] for c in catalog]
        self.assertEqual(len(measurements), len(set(measurements)))

    def test_catalog_display_names_match_sync_summary(self):
        """Display names should match the names used in print_sync_summary."""
        catalog = get_data_catalog()
        display_names = {c["display_name"] for c in catalog}
        expected = {
            "daily stats", "sleep", "heart rate", "body composition",
            "respiration", "SpO2", "stress", "HRV", "hydration",
            "training readiness", "training status", "max metrics",
            "endurance score", "hill score", "fitness age", "floors",
            "activities",
        }
        self.assertEqual(display_names, expected)

    def test_catalog_tags_are_lists(self):
        catalog = get_data_catalog()
        for entry in catalog:
            self.assertIsInstance(entry["tags"], list)

    def test_print_data_catalog_output(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_catalog()
            output = mock_out.getvalue()
        self.assertIn("catGar Data Catalog", output)
        self.assertIn("QUICK REFERENCE", output)
        self.assertIn("DETAILED FIELD REFERENCE", output)
        self.assertIn("USAGE NOTES", output)
        self.assertIn("daily_stats", output)
        self.assertIn("Total data categories: 17", output)

    def test_print_data_catalog_includes_all_measurements(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_catalog()
            output = mock_out.getvalue()
        catalog = get_data_catalog()
        for entry in catalog:
            self.assertIn(entry["measurement"], output)


class TestComputeFieldStats(unittest.TestCase):
    def test_basic_stats(self):
        vals = [10.0, 20.0, 30.0, 40.0, 50.0]
        st = compute_field_stats(vals)
        self.assertEqual(st["count"], 5)
        self.assertEqual(st["min"], 10.0)
        self.assertEqual(st["max"], 50.0)
        self.assertAlmostEqual(st["mean"], 30.0)
        self.assertAlmostEqual(st["median"], 30.0)
        self.assertGreater(st["stdev"], 0)

    def test_single_value(self):
        st = compute_field_stats([42.0])
        self.assertEqual(st["count"], 1)
        self.assertEqual(st["min"], 42.0)
        self.assertEqual(st["max"], 42.0)
        self.assertAlmostEqual(st["mean"], 42.0)
        self.assertAlmostEqual(st["median"], 42.0)
        self.assertEqual(st["stdev"], 0.0)

    def test_empty_list(self):
        self.assertIsNone(compute_field_stats([]))

    def test_two_values(self):
        st = compute_field_stats([10.0, 20.0])
        self.assertEqual(st["count"], 2)
        self.assertAlmostEqual(st["mean"], 15.0)
        self.assertAlmostEqual(st["median"], 15.0)
        self.assertGreater(st["stdev"], 0)

    def test_identical_values(self):
        st = compute_field_stats([5.0, 5.0, 5.0])
        self.assertEqual(st["min"], 5.0)
        self.assertEqual(st["max"], 5.0)
        self.assertAlmostEqual(st["stdev"], 0.0)


class TestFormatStatValue(unittest.TestCase):
    def test_integer_value(self):
        self.assertEqual(_format_stat_value(10.0), "10")

    def test_float_value(self):
        self.assertEqual(_format_stat_value(10.5), "10.5")

    def test_large_float(self):
        result = _format_stat_value(1234567.89)
        self.assertEqual(result, "1234567.9")

    def test_zero(self):
        self.assertEqual(_format_stat_value(0.0), "0")


class TestBuildHistogram(unittest.TestCase):
    def test_basic_histogram(self):
        vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        lines = _build_histogram(vals, bins=5, width=20)
        self.assertEqual(len(lines), 5)
        for line in lines:
            self.assertIsInstance(line, str)

    def test_single_value(self):
        lines = _build_histogram([5.0], bins=5, width=20)
        self.assertEqual(len(lines), 1)
        self.assertIn("5", lines[0])

    def test_empty_values(self):
        lines = _build_histogram([], bins=5, width=20)
        self.assertEqual(lines, [])

    def test_identical_values(self):
        lines = _build_histogram([3.0, 3.0, 3.0], bins=5, width=20)
        self.assertEqual(len(lines), 1)
        self.assertIn("3", lines[0])

    def test_histogram_contains_counts(self):
        vals = [1.0, 1.0, 1.0, 5.0]
        lines = _build_histogram(vals, bins=2, width=10)
        self.assertEqual(len(lines), 2)
        # First bin should have 3 values
        self.assertIn("3", lines[0])


class TestPrintDataSummary(unittest.TestCase):
    def test_output_contains_header(self):
        summary = {
            "daily_stats": {
                "days_with_data": 5,
                "total_points": 90,
                "fields": {
                    "steps": [8000.0, 9000.0, 7000.0, 10000.0, 8500.0],
                    "resting_hr": [58.0, 60.0, 57.0, 59.0, 61.0],
                },
                "tag_values": {},
            },
        }
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_summary(summary, 7)
            output = mock_out.getvalue()
        self.assertIn("catGar Data Summary", output)
        self.assertIn("last 7 days", output)
        self.assertIn("DATA AVAILABILITY", output)
        self.assertIn("FIELD STATISTICS", output)

    def test_output_shows_measurement_stats(self):
        summary = {
            "daily_stats": {
                "days_with_data": 3,
                "total_points": 54,
                "fields": {
                    "steps": [8000.0, 9000.0, 10000.0],
                },
                "tag_values": {},
            },
        }
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_summary(summary, 7)
            output = mock_out.getvalue()
        self.assertIn("daily_stats", output)
        self.assertIn("steps", output)
        self.assertIn("DAILY STATS", output)

    def test_output_empty_summary(self):
        summary = {}
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_summary(summary, 7)
            output = mock_out.getvalue()
        self.assertIn("catGar Data Summary", output)
        self.assertIn("Measurements with data: 0/", output)

    def test_output_shows_distributions(self):
        summary = {
            "daily_stats": {
                "days_with_data": 5,
                "total_points": 90,
                "fields": {
                    "steps": [8000.0, 9000.0, 7000.0, 10000.0, 8500.0],
                    "resting_hr": [58.0, 60.0, 57.0, 59.0, 61.0],
                    "avg_stress": [25.0, 30.0, 28.0, 35.0, 22.0],
                },
                "tag_values": {},
            },
        }
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_summary(summary, 7)
            output = mock_out.getvalue()
        self.assertIn("DISTRIBUTIONS", output)
        self.assertIn("Daily Steps", output)

    def test_output_shows_tag_values(self):
        from collections import Counter
        summary = {
            "activity": {
                "days_with_data": 3,
                "total_points": 6,
                "fields": {
                    "distance_meters": [5000.0, 3000.0, 8000.0],
                },
                "tag_values": {
                    "type": Counter({"running": 4, "cycling": 2}),
                },
            },
        }
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_data_summary(summary, 14)
            output = mock_out.getvalue()
        self.assertIn("running", output)
        self.assertIn("cycling", output)
        self.assertIn("last 14 days", output)


class TestQueryDataSummary(unittest.TestCase):
    def test_queries_all_measurements(self):
        """Verify query_data_summary queries for every measurement in catalog."""
        catalog = get_data_catalog()
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mock_query_api.query.return_value = []

        result = query_data_summary(mock_client, "garmin", 7)

        # Should have an entry for every measurement
        for cat in catalog:
            self.assertIn(cat["measurement"], result)

    def test_handles_query_errors_gracefully(self):
        """Verify query_data_summary continues on query errors."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mock_query_api.query.side_effect = Exception("connection refused")

        result = query_data_summary(mock_client, "garmin", 7)

        # Should still return entries (empty) for each measurement
        catalog = get_data_catalog()
        self.assertEqual(len(result), len(catalog))
        for meas, entry in result.items():
            self.assertEqual(entry["days_with_data"], 0)
            self.assertEqual(entry["total_points"], 0)
            self.assertEqual(entry["fields"], {})

    def test_empty_result_structure(self):
        """Verify each entry has expected keys."""
        mock_client = MagicMock()
        mock_query_api = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        mock_query_api.query.return_value = []

        result = query_data_summary(mock_client, "garmin", 30)

        for meas, entry in result.items():
            self.assertIn("days_with_data", entry)
            self.assertIn("total_points", entry)
            self.assertIn("fields", entry)
            self.assertIn("tag_values", entry)


if __name__ == "__main__":
    unittest.main()
