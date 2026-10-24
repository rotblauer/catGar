"""Max metrics, training status, training readiness, HRV and all-day heart rate in
the shapes Garmin Connect returns them: VO2 max nested per sport, training status
and load nested per device, readiness as a list of snapshots, heart rate as one dict
for the day. Before the fix the first two stored only userId, readiness raised on
any non-empty day and heart rate stored nothing. Also --only, the auto-mode start
day and when the last-synced day is recorded.
All values are made up."""
import io
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import catgar
from catgar import (
    COLLECTOR_NAMES,
    auto_sync_start,
    build_heart_rate_points,
    build_hrv_points,
    build_max_metrics_points,
    build_training_readiness_points,
    build_training_status_points,
    fetch_and_write,
    last_sync_after,
    parse_only,
    print_sync_summary,
)


def fields(points, measurement=None):
    """{field: value} over the points (line protocol parsed back)."""
    out = {}
    for p in points:
        line = p.to_line_protocol()
        name, rest = line.split(" ", 1)
        if measurement and not name.startswith(measurement):
            continue
        body = rest.rsplit(" ", 1)[0]
        for kv in body.split(","):
            k, v = kv.split("=", 1)
            out[k] = float(v.rstrip("i")) if v[0].isdigit() or v[0] == "-" else v.strip('"')
    return out


def stamps(points, ms=False):
    """The points' times, in seconds (*ms*: the points were written in ms)."""
    return [int(p.to_line_protocol().rsplit(" ", 1)[1]) // (1000 if ms else 1) for p in points]


def without_warnings(test, fn, *args):
    """fn(*args), failing *test* if it logged a warning."""
    with patch.object(catgar.log, "warning") as warn:
        out = fn(*args)
    test.assertFalse(warn.called, warn.call_args_list)
    return out


def utc_seconds(y, mo, d, h=0, mi=0):
    return int(datetime(y, mo, d, h, mi, tzinfo=timezone.utc).timestamp())


MAX_METRICS = [{
    "userId": 1234567,
    "generic": {"calendarDate": "2001-03-02", "vo2MaxPreciseValue": 51.7, "vo2MaxValue": 52.0,
                "fitnessAge": None, "fitnessAgeDescription": None, "maxMetCategory": 0},
    "cycling": None,
    "heatAltitudeAcclimation": {"calendarDate": "2001-03-02", "altitudeAcclimationDate": "2001-03-02",
                                "previousAltitudeAcclimationDate": "2001-03-01", "heatAcclimationDate": "2001-03-02",
                                "previousHeatAcclimationDate": "2001-03-01", "altitudeAcclimation": 0,
                                "previousAltitudeAcclimation": 0, "heatAcclimationPercentage": 12,
                                "previousHeatAcclimationPercentage": 10, "heatTrend": "ACCLIMATIZING",
                                "altitudeTrend": None, "currentAltitude": 250, "previousAltitude": 250,
                                "acclimationPercentage": 12, "previousAcclimationPercentage": 10,
                                "altitudeAcclimationLocalTimestamp": "2001-03-02T07:00:00.0"},
}]

TRAINING_STATUS = {
    "userId": 1234567,
    "mostRecentVO2Max": {"userId": 1234567, "generic": {"vo2MaxPreciseValue": 51.7, "vo2MaxValue": 52.0}},
    "mostRecentTrainingLoadBalance": {
        "userId": 1234567,
        "metricsTrainingLoadBalanceDTOMap": {
            "3999000111": {"calendarDate": "2001-03-02", "deviceId": 3999000111,
                           "monthlyLoadAerobicLow": 410.5, "monthlyLoadAerobicHigh": 520.0,
                           "monthlyLoadAnaerobic": 95.2, "monthlyLoadAerobicLowTargetMin": 300,
                           "monthlyLoadAerobicLowTargetMax": 600, "monthlyLoadAerobicHighTargetMin": 350,
                           "monthlyLoadAerobicHighTargetMax": 700, "monthlyLoadAnaerobicTargetMin": 0,
                           "monthlyLoadAnaerobicTargetMax": 150, "trainingBalanceFeedbackPhrase": "BALANCED",
                           "primaryTrainingDevice": True},
        },
        "recordedDevices": [{"deviceId": 3999000111, "deviceName": "Watch"}],
    },
    "mostRecentTrainingStatus": {
        "userId": 1234567,
        "latestTrainingStatusData": {
            "3999000111": {"calendarDate": "2001-03-02", "sinceDate": "2001-02-20", "weeklyTrainingLoad": 612,
                           "trainingStatus": 4, "timestamp": 983541600000, "deviceId": 3999000111,
                           "loadTunnelMin": 400, "loadTunnelMax": 900, "loadLevelTrend": None,
                           "sport": "RUNNING", "subSport": "GENERIC", "fitnessTrendSport": "RUNNING",
                           "fitnessTrend": 2, "trainingStatusFeedbackPhrase": "PRODUCTIVE_1",
                           "trainingPaused": False, "primaryTrainingDevice": True,
                           "acuteTrainingLoadDTO": {"acwrPercent": 55, "acwrStatus": "OPTIMAL",
                                                    "dailyTrainingLoadAcute": 640, "maxTrainingLoadChronic": 800.5,
                                                    "minTrainingLoadChronic": 430.0, "dailyTrainingLoadChronic": 590,
                                                    "dailyAcuteChronicWorkloadRatio": 1.1}},
            "3999000222": {"weeklyTrainingLoad": 9999, "trainingStatus": 7, "primaryTrainingDevice": False},
        },
        "recordedDevices": [{"deviceId": 3999000111}],
        "showSelector": False,
    },
    "heatAltitudeAcclimationDTO": None,
}


def readiness_snapshot(context, local, score):
    return {"userProfilePK": 1234567, "calendarDate": "2001-03-02", "timestampLocal": local,
            "timestamp": local, "deviceId": 3999000111, "level": "MODERATE", "feedbackShort": "TEXT",
            "score": score, "sleepScore": 80, "sleepScoreFactorPercent": 70, "recoveryTime": 600,
            "acuteLoad": 300, "hrvFactorPercent": 90, "hrvWeeklyAverage": 60, "validSleep": True,
            "inputContext": context, "primaryActivityTracker": True}


class TestMaxMetricsNested(unittest.TestCase):
    def test_vo2_per_sport_and_acclimation(self):
        pts = without_warnings(self, build_max_metrics_points, MAX_METRICS, "2001-03-02")
        lines = [p.to_line_protocol() for p in pts]
        vo2 = [l for l in lines if l.startswith("max_metrics")]
        self.assertEqual(len(vo2), 1)                       # cycling is None: no point
        self.assertIn("sport=generic", vo2[0])
        self.assertIn("vo2max_precise=51.7", vo2[0])
        self.assertIn("vo2max=52", vo2[0])
        self.assertNotIn("userId", " ".join(lines))          # an identifier, not a reading
        self.assertNotIn("maxMetCategory", " ".join(lines))
        acc = fields(pts, "heat_altitude_acclimation")
        self.assertEqual(acc, {"heat_acclimation_pct": 12, "altitude_acclimation": 0,
                               "acclimation_pct": 12, "current_altitude": 250})

    def test_cycling_block_gets_its_own_point(self):
        data = [dict(MAX_METRICS[0], cycling={"vo2MaxPreciseValue": 48.0, "vo2MaxValue": 48.0})]
        sports = sorted(l.split(",")[1].split(" ")[0] for l in
                        (p.to_line_protocol() for p in build_max_metrics_points(data, "2001-03-02"))
                        if l.startswith("max_metrics"))
        self.assertEqual(sports, ["sport=cycling", "sport=generic"])

    def test_only_user_id_writes_nothing(self):
        self.assertEqual(build_max_metrics_points([{"userId": 1234567}], "2001-03-02"), [])


class TestTrainingStatusNested(unittest.TestCase):
    def test_primary_device_status_load_and_balance(self):
        # text fields (sport, dates) are skipped without a warning
        pts = without_warnings(self, build_training_status_points, TRAINING_STATUS, "2001-03-02")
        f = fields(pts)
        self.assertEqual(f["status"], 4)
        self.assertEqual(f["weekly_load"], 612)
        self.assertEqual(f["load_tunnel_min"], 400)
        self.assertEqual(f["acute_load"], 640)
        self.assertEqual(f["chronic_load"], 590)
        self.assertEqual(f["acwr"], 1.1)
        self.assertEqual(f["load_aerobic_low"], 410.5)
        self.assertEqual(f["load_anaerobic_target_max"], 150)
        self.assertEqual(f["status_phrase"], "PRODUCTIVE_1")   # Garmin's own label, as text
        self.assertEqual(f["balance_phrase"], "BALANCED")
        self.assertNotIn("userId", f)
        self.assertNotIn("primaryTrainingDevice", " ".join(f))
        self.assertNotIn("status_trainingPaused", f)        # booleans aren't readings
        self.assertNotIn("status_sport", f)
        self.assertNotEqual(f["weekly_load"], 9999)          # the secondary device is ignored
        self.assertEqual(set(stamps(pts)), {utc_seconds(2001, 3, 2)})

    def test_only_user_id_writes_nothing(self):
        self.assertEqual(build_training_status_points({"userId": 1234567}, "2001-03-02"), [])

    def test_no_acute_load_block(self):
        # as a watch without acute-load support returns it: acuteTrainingLoadDTO is null
        data = {**TRAINING_STATUS, "mostRecentTrainingStatus": {
            **TRAINING_STATUS["mostRecentTrainingStatus"],
            "latestTrainingStatusData": {"3999000111": {
                **TRAINING_STATUS["mostRecentTrainingStatus"]["latestTrainingStatusData"]["3999000111"],
                "acuteTrainingLoadDTO": None, "loadLevelTrend": 1}}}}
        f = fields(build_training_status_points(data, "2001-03-02"))
        self.assertEqual(f["status"], 4)
        self.assertEqual(f["load_level_trend"], 1)
        self.assertNotIn("acute_load", f)
        self.assertEqual(f["load_aerobic_high"], 520.0)

    def test_older_entry_is_stamped_at_its_own_day(self):
        # asked for 03-05, Garmin's most recent entries are from 03-02 (no sync since)
        pts = build_training_status_points(TRAINING_STATUS, "2001-03-05")
        self.assertEqual(set(stamps(pts)), {utc_seconds(2001, 3, 2)})


class TestTrainingReadinessList(unittest.TestCase):
    def test_morning_snapshot_of_the_list(self):
        data = [readiness_snapshot("UPDATE_REALTIME_VARIABLES", "2001-03-02T21:00:00.0", 40),
                readiness_snapshot("AFTER_WAKEUP_RESET", "2001-03-02T06:30:00.0", 75),
                readiness_snapshot("UPDATE_REALTIME_VARIABLES", "2001-03-02T02:00:00.0", 60)]
        f = fields(without_warnings(self, build_training_readiness_points, data, "2001-03-02"))
        self.assertEqual(f["score"], 75)
        self.assertEqual(f["recovery_time"], 600)
        self.assertEqual(f["sleep_factor_pct"], 70)
        self.assertEqual(f["hrv_weekly_avg"], 60)
        for key in ("userProfilePK", "deviceId", "validSleep", "level"):
            self.assertNotIn(key, f)

    def test_latest_snapshot_without_a_morning_one(self):
        data = [readiness_snapshot("AFTER_POST_EXERCISE_RESET", "2001-03-02T09:00:00.0", 55),
                readiness_snapshot("UPDATE_REALTIME_VARIABLES", "2001-03-02T21:00:00.0", 40)]
        self.assertEqual(fields(build_training_readiness_points(data, "2001-03-02"))["score"], 40)

    def test_empty_list(self):
        self.assertEqual(build_training_readiness_points([], "2001-03-02"), [])


class TestHrvNull(unittest.TestCase):
    def test_null_summary_writes_nothing(self):
        self.assertEqual(build_hrv_points({"hrvSummary": None, "hrvReadings": []}, "2001-03-02"), [])


class TestHeartRateDict(unittest.TestCase):
    def test_one_dict_per_day_shifted_to_local_digits(self):
        # 2001-03-02 local midnight in a UTC-6 zone is 06:00 GMT
        gmt_ms = utc_seconds(2001, 3, 2, 6) * 1000
        data = {"userProfilePK": 1234567, "calendarDate": "2001-03-02",
                "startTimestampGMT": "2001-03-02T06:00:00.0", "startTimestampLocal": "2001-03-02T00:00:00.0",
                "endTimestampGMT": "2001-03-03T05:59:59.0", "endTimestampLocal": "2001-03-02T23:59:59.0",
                "maxHeartRate": 150, "minHeartRate": 48, "restingHeartRate": 52,
                "heartRateValues": [[gmt_ms, 55], [gmt_ms + 120000, None], [gmt_ms + 240000, 57]]}
        pts = build_heart_rate_points(data, "2001-03-02")
        self.assertEqual(len(pts), 2)                        # the None sample is skipped
        self.assertIn("bpm=55i", pts[0].to_line_protocol())
        # stored as the local clock digits written as UTC: 2001-03-02T00:00:00Z
        self.assertEqual(stamps(pts, ms=True), [utc_seconds(2001, 3, 2), utc_seconds(2001, 3, 2, 0, 4)])

    def test_clock_change_day_goes_through_the_zone(self):
        # 2001-10-28 in Europe/Berlin: +02:00 until 01:00 GMT, then +01:00
        tz = ZoneInfo("Europe/Berlin")
        data = {"startTimestampGMT": "2001-10-27T22:00:00.0", "startTimestampLocal": "2001-10-28T00:00:00.0",
                "endTimestampGMT": "2001-10-28T22:59:59.0", "endTimestampLocal": "2001-10-28T23:59:59.0",
                "heartRateValues": [[utc_seconds(2001, 10, 27, 22) * 1000, 50],
                                    [utc_seconds(2001, 10, 28, 12) * 1000, 70]]}
        pts = build_heart_rate_points(data, "2001-10-28", tz=tz)
        self.assertEqual(stamps(pts, ms=True), [utc_seconds(2001, 10, 28, 0), utc_seconds(2001, 10, 28, 13)])

    def test_missing_stamps_use_the_zone(self):
        data = {"heartRateValues": [[utc_seconds(2001, 3, 2, 10) * 1000, 60]]}
        with self.assertLogs("catgar", level="WARNING"):
            pts = build_heart_rate_points(data, "2001-03-02", tz=timezone(timedelta(hours=2)))
        self.assertEqual(stamps(pts, ms=True), [utc_seconds(2001, 3, 2, 12)])

    def test_none_or_empty(self):
        self.assertEqual(build_heart_rate_points(None, "2001-03-02"), [])
        self.assertEqual(build_heart_rate_points({"heartRateValues": None}, "2001-03-02"), [])


class TestOnlyAndAutoStart(unittest.TestCase):
    def test_parse_only(self):
        self.assertEqual(parse_only("Daily Stats, sleep"), {"daily stats", "sleep"})
        for bad in ("daily stats, steps", " , ", ""):
            with self.assertRaises(ValueError):
                parse_only(bad)

    def test_every_name_pulls_exactly_its_category(self):
        for name in COLLECTOR_NAMES:
            garmin = MagicMock()
            garmin.get_activities_by_date.return_value = []
            fetch_and_write(garmin, MagicMock(), "bucket", "org", "2001-03-02", only={name})
            called = [c[0] for c in garmin.method_calls]
            self.assertEqual(len(called), 1, (name, called))

    def test_summary_marks_skipped_categories(self):
        out = io.StringIO()
        with redirect_stdout(out):
            print_sync_summary({"sleep": 3}, 1, [], only={"sleep"})
        text = out.getvalue()
        self.assertIn("✓ sleep", text)
        self.assertIn("skipped", text)
        self.assertEqual(text.count("skipped"), len(COLLECTOR_NAMES) - 1)

    def test_auto_start_re_pulls_the_day_before(self):
        self.assertEqual(auto_sync_start(date(2001, 3, 2), date(2001, 3, 2)), date(2001, 3, 1))
        self.assertEqual(auto_sync_start(date(2001, 2, 20), date(2001, 3, 2)), date(2001, 2, 19))
        self.assertEqual(auto_sync_start(None, date(2001, 3, 2)), date(2001, 3, 2))

    def test_last_sync_only_after_a_whole_run_and_never_backwards(self):
        self.assertEqual(last_sync_after(date(2001, 3, 1), date(2001, 3, 2), True), date(2001, 3, 2))
        self.assertEqual(last_sync_after(None, date(2001, 3, 2), True), date(2001, 3, 2))
        self.assertEqual(last_sync_after(date(2001, 3, 9), date(2001, 3, 2), True), date(2001, 3, 9))
        self.assertIsNone(last_sync_after(date(2001, 2, 20), date(2001, 3, 2), False))


if __name__ == "__main__":
    unittest.main()
