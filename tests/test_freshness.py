from datetime import date

from job_sl.freshness import evaluate

TODAY = date(2026, 10, 20)


def _days(*ds, size=100):
    return [(d, size) for d in ds]


def test_stale_source_alerts():
    l2 = {"a": _days("20260909"), "b": _days("20261020")}
    rows = {r["source"]: r for r in evaluate({}, l2, TODAY)}
    assert rows["a"]["level"] == "alert" and rows["a"]["l2_age_days"] == 41
    assert rows["b"]["level"] == "ok"


def test_yesterday_is_ok_two_days_old_alerts():
    l2 = {"a": _days("20261019"), "b": _days("20261018")}
    rows = {r["source"]: r for r in evaluate({}, l2, TODAY)}
    assert rows["a"]["level"] == "ok"
    assert rows["b"]["level"] == "alert"


def test_l1_without_l2_alerts_and_ignore():
    rows = evaluate({"x": _days("20261020"), "workport": _days("20260713")}, {}, TODAY)
    assert [r["source"] for r in rows] == ["x"] and rows[0]["level"] == "alert"


def test_shrink_warns_and_static_warns():
    l2 = {"s": [("20261019", 1000), ("20261020", 100)]}
    assert evaluate({}, l2, TODAY)[0]["level"] == "warn"
    days = [f"202610{d:02d}" for d in range(6, 21)]
    rows = evaluate({}, {"t": _days(*days, size=500)}, TODAY)
    assert rows[0]["level"] == "warn" and "不変" in rows[0]["reasons"][0]
    rows = evaluate({"t": _days("20261015")}, {"t": _days(*days, size=500)}, TODAY)
    assert rows[0]["level"] == "ok"
