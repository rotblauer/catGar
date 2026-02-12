"""Unit tests for catgar data transformation functions."""

import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

from catgar import (
    _safe_float,
    build_activity_points,
    build_body_composition_points,
    build_daily_stats_points,
    build_heart_rate_points,
    build_respiration_points,
    build_sleep_points,
    build_spo2_points,
    ensure_bucket,
    find_oldest_available_date,
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


if __name__ == "__main__":
    unittest.main()
