"""Body composition as get_body_composition() really returns it: weigh-ins nested
under dateWeightList, the period mean under totalAverage (weight-service
/weight/dateRange). Before the fix these produced zero points.
All values are made up."""
import unittest
from datetime import datetime, timezone

from catgar import build_body_composition_points


def local_ms(s):
    """Epoch ms of a local wall-clock time, the way Garmin's `date` field carries it."""
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp() * 1000)


def secs(s):
    return str(int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp()))


def weigh_in(when, weight, gmt_offset_h=-2, **extra):
    rec = {
        "samplePk": local_ms(when) + gmt_offset_h * 3600_000,
        "date": local_ms(when),
        "calendarDate": when[:10],
        "weight": weight, "bmi": 22.0, "bodyFat": 20.0, "bodyWater": 55.0,
        "boneMass": 3000.0, "muscleMass": 30000.0,
        "physiqueRating": None, "visceralFat": None, "metabolicAge": None,
        "sourceType": "INDEX_SCALE",
        "timestampGMT": local_ms(when) + gmt_offset_h * 3600_000,
        "weightDelta": -100.0,
    }
    rec.update(extra)
    return rec


def nested(day, entries, avg_weight=None):
    return {
        "startDate": day, "endDate": day,
        "dateWeightList": entries,
        "totalAverage": {"from": local_ms(day + " 00:00"), "until": local_ms(day + " 23:59"),
                         "weight": avg_weight, "bmi": None, "bodyFat": None},
    }


class TestNestedBodyComposition(unittest.TestCase):
    def test_one_weigh_in_is_written_at_its_local_time(self):
        data = nested("2001-03-02", [weigh_in("2001-03-02 07:05", 70000.0)], 70000.0)
        pts = build_body_composition_points(data, "2001-03-02")
        lp = "\n".join(p.to_line_protocol() for p in pts)
        self.assertIn("weight_grams=70000", lp)
        self.assertIn("body_fat_pct=20", lp)
        self.assertIn("weight_change=-100", lp)
        self.assertIn("source=INDEX_SCALE", lp)
        self.assertTrue(all(l.endswith(secs("2001-03-02 07:05")) for l in lp.splitlines()))

    def test_metadata_is_never_a_field(self):
        data = nested("2001-03-02", [weigh_in("2001-03-02 07:05", 70000.0)], 70000.0)
        lp = "\n".join(p.to_line_protocol() for p in build_body_composition_points(data, "2001-03-02"))
        for key in ("samplePk", "timestampGMT", " date=", "from=", "until=", "calendarDate", "sourceType="):
            self.assertNotIn(key, lp)
        # weight, bmi, fat, water, bone, muscle, delta: 7 fields, nulls skipped
        self.assertEqual(len(lp.splitlines()), 7)

    def test_two_weigh_ins_keep_two_times(self):
        data = nested("2001-03-02", [weigh_in("2001-03-02 07:05", 70000.0),
                                     weigh_in("2001-03-02 21:40", 71000.0)], 70500.0)
        pts = build_body_composition_points(data, "2001-03-02")
        stamps = {p.to_line_protocol().rsplit(" ", 1)[1] for p in pts}
        self.assertEqual(stamps, {secs("2001-03-02 07:05"), secs("2001-03-02 21:40")})

    def test_day_without_weigh_ins_writes_nothing(self):
        data = nested("2001-03-03", [], None)
        self.assertEqual(build_body_composition_points(data, "2001-03-03"), [])
        data = {"startDate": "2001-03-03", "endDate": "2001-03-03", "dateWeightList": [], "totalAverage": None}
        self.assertEqual(build_body_composition_points(data, "2001-03-03"), [])

    def test_total_average_only_falls_back_to_the_day(self):
        data = {"startDate": "2001-03-02", "endDate": "2001-03-02",
                "totalAverage": {"from": 1, "until": 2, "weight": 69500.0, "bmi": 22.0}}
        lp = "\n".join(p.to_line_protocol() for p in build_body_composition_points(data, "2001-03-02"))
        self.assertIn("weight_grams=69500", lp)
        self.assertNotIn("from=", lp)
        self.assertTrue(all(l.endswith(secs("2001-03-02 00:00")) for l in lp.splitlines()))

    def test_missing_local_time_uses_calendar_date(self):
        rec = weigh_in("2001-03-01 06:30", 70000.0)
        del rec["date"]
        pts = build_body_composition_points(nested("2001-03-02", [rec]), "2001-03-02")
        self.assertTrue(all(p.to_line_protocol().endswith(secs("2001-03-01 00:00")) for p in pts))

    def test_multi_day_range_for_a_backfill(self):
        data = {"startDate": "2000-01-01", "endDate": "2001-03-02",
                "dateWeightList": [weigh_in("2000-01-02 07:00", 72000.0),
                                   weigh_in("2000-06-15 06:45", 71000.0),
                                   weigh_in("2001-03-02 07:05", 70000.0, sourceType="MANUAL")],
                "totalAverage": {"weight": 71000.0}}
        pts = build_body_composition_points(data, "2001-03-02")
        stamps = {p.to_line_protocol().rsplit(" ", 1)[1] for p in pts}
        self.assertEqual(stamps, {secs("2000-01-02 07:00"), secs("2000-06-15 06:45"), secs("2001-03-02 07:05")})
        self.assertTrue(any("source=MANUAL" in p.to_line_protocol() for p in pts))


if __name__ == "__main__":
    unittest.main()
