import io
from collections import Counter
from datetime import date

import pytest

import job_sl as jsl
from job_sl import closure, migrate_state
from job_sl.closure import St

TODAY = date(2026, 10, 20)


def st(
    status="active",
    reason="seen",
    last="2026-10-19",
    shard="",
    closed="",
    first="2026-09-01",
    src="s",
    misses="0",
):
    return St(src, first, last, shard, status, reason, closed, misses)


def seen_of(source, shards, expected=None):
    ss = closure.SourceSeen(
        source, list(expected if expected is not None else sorted(shards))
    )
    for sh, urls in shards.items():
        ss.shard_urls[sh] = set(urls)
    return ss


def plan_judge():
    return closure.SourcePlan("s", "judge")


def test_parse_seen_lines_handles_bom_header_and_case():
    lines = ["﻿job_url\r\n", "https://X.com/A \r\n", "\r\n", "https://x.com/b\r\n"]
    assert list(closure.parse_seen_lines(lines)) == [
        "https://x.com/a",
        "https://x.com/b",
    ]


def test_seen_url_is_active_and_clears_closed():
    prev = st("closed", "not_seen", closed="2026-10-01")
    r = closure.decide("k", "s", prev, 2, plan_judge(), "", TODAY)
    assert (r.status, r.reason, r.last_seen, r.last_shard, r.closed_date) == (
        "active",
        "seen",
        "2026-10-20",
        "2",
        "",
    )
    assert r.first_seen == "2026-09-01"


def test_unseen_on_completed_crawl_is_closed_after_consecutive_misses():
    # 1回目は保留(1日欠けて戻る求人が townwork/rikunabi で1〜4割ある)。連続2回目で閉じ、検知日はその日
    r = closure.decide("k", "s", st(), None, plan_judge(), "", TODAY)
    assert (r.status, r.reason, r.misses, r.closed_date) == (
        "active",
        "miss_pending",
        "1",
        "",
    )
    r = closure.decide("k", "s", r, None, plan_judge(), "", TODAY)
    assert (r.status, r.reason, r.closed_date) == ("closed", "not_seen", "2026-10-20")


def test_miss_counter_resets_when_seen_again():
    r = closure.decide("k", "s", st(misses="1"), 0, plan_judge(), "", TODAY)
    assert (r.status, r.misses) == ("active", "0")


def test_past_valid_through_closes_without_waiting_for_second_miss():
    r = closure.decide("k", "s", st(), None, plan_judge(), "2026-10-01", TODAY)
    assert (r.status, r.reason) == ("closed", "not_seen")


def test_closed_detected_date_is_never_overwritten():
    prev = st("closed", "not_seen", closed="2026-10-01")
    r = closure.decide("k", "s", prev, None, plan_judge(), "", TODAY)
    assert (r.status, r.closed_date) == ("closed", "2026-10-01")


def test_never_seen_row_uses_past_valid_through_as_closed_date():
    prev = st("unknown", "migrated", last="")
    r = closure.decide("k", "s", prev, None, plan_judge(), "2026-07-22", TODAY)
    assert (r.status, r.closed_date) == ("closed", "2026-07-22")
    # valid_throughが未来なら移行直後の行も2回続けて確認できなかったときに閉じ、検知日はその日
    r = closure.decide("k", "s", prev, None, plan_judge(), "2027-07-22", TODAY)
    assert (r.status, r.reason) == ("unknown", "miss_pending")
    r = closure.decide("k", "s", r, None, plan_judge(), "2027-07-22", TODAY)
    assert (r.status, r.closed_date) == ("closed", "2026-10-20")


def test_hold_keeps_active_for_a_few_days_then_unknown():
    hold = closure.SourcePlan("s", "hold", "no_seen_file")
    r = closure.decide("k", "s", st(last="2026-10-18"), None, hold, "", TODAY)
    assert (r.status, r.reason) == ("active", "no_seen_file")
    r = closure.decide("k", "s", st(last="2026-10-10"), None, hold, "", TODAY)
    assert (r.status, r.reason) == ("unknown", "stale_unconfirmed")


def test_hold_with_past_valid_through_closes_as_auxiliary():
    hold = closure.SourcePlan("s", "hold", "no_seen_file")
    r = closure.decide("k", "s", st(last="2026-10-19"), None, hold, "2026-10-01", TODAY)
    assert (r.status, r.reason) == ("closed", "valid_through_expired")


def test_source_without_seen_is_unknown_with_reason():
    r = closure.decide("k", "s", None, None, None, "", TODAY)
    assert (r.status, r.reason, r.first_seen) == (
        "unknown",
        "no_seen_source",
        "2026-10-20",
    )
    r = closure.decide("k", "s", None, None, None, "2026-01-01", TODAY)
    assert (r.status, r.reason) == ("closed", "valid_through_expired")


def test_partial_plan_judges_only_rows_of_judged_shards():
    plan = closure.SourcePlan("s", "partial", "shard_incomplete", judged_shards={1})
    assert (
        closure.decide(
            "k", "s", st(shard="1", misses="1"), None, plan, "", TODAY
        ).status
        == "closed"
    )
    held = closure.decide("k", "s", st(shard="0"), None, plan, "", TODAY)
    assert (held.status, held.reason) == ("active", "shard_incomplete")
    unknown_shard = closure.decide("k", "s", st(shard=""), None, plan, "", TODAY)
    assert unknown_shard.status == "active"


def test_plan_no_seen_file_and_safety_denominator_is_active_count():
    assert closure.plan_source("s", None, 100, Counter(), None).reason == "no_seen_file"
    full = seen_of("s", {0: {f"u{i}" for i in range(60)}})
    # 前日active 100 に対し確認60 -> 60% < 70% で保留
    assert closure.plan_source("s", full, 100, Counter(), None).reason == "safety_ratio"
    # closed/凍結行が山ほどあっても分母はactiveだけ(旧実装は累積件数が分母で恒常スキップしていた)
    assert closure.plan_source("s", full, 70, Counter(), None).mode == "judge"
    assert (
        closure.plan_source("s", seen_of("s", {0: set()}), 0, Counter(), None).mode
        == "hold"
    )


def test_plan_missing_shard_stable_vs_unstable():
    seen = seen_of("s", {0: {"a"}, 2: {"c"}}, expected=[0, 1, 2])
    by_shard = Counter({"0": 1, "1": 1, "2": 1})
    stable = closure.plan_source("s", seen, 3, by_shard, 1.0)
    assert (stable.mode, stable.judged_shards) == ("partial", {0, 2})
    unstable = closure.plan_source("s", seen, 3, by_shard, 0.3)
    assert (unstable.mode, unstable.reason) == ("hold", "source_partial_unstable")
    nobase = closure.plan_source("s", seen, 3, by_shard, None)
    assert nobase.mode == "hold"


def test_plan_partial_drops_shard_below_ratio():
    seen = seen_of("s", {0: {"a"}, 2: {"c"}}, expected=[0, 1, 2])
    plan = closure.plan_source("s", seen, 30, Counter({"0": 10, "1": 10, "2": 10}), 1.0)
    assert plan.judged_shards == set()  # 各シャード1/10 < 70%


def test_shard_stability():
    prev = {"a": st(shard="0"), "b": st(shard="1"), "c": st(shard="")}
    seen = seen_of("s", {0: {"a", "b"}, 1: {"c", "new"}}, expected=[0, 1, 2])
    assert closure.shard_stability(prev, seen) == 0.5  # aは同じ、bは移動。cは比較不能
    assert closure.shard_stability({}, seen) is None


def test_suspect_cap_warning():
    seen = seen_of("s", {0: {f"u{i}" for i in range(100_000)}})
    plan = closure.plan_source("s", seen, 100_000, Counter(), None)
    assert plan.mode == "judge" and plan.anomalies


def test_judge_end_to_end_with_stable_partial():
    prev = {
        "a": st(shard="0", src="m"),
        "b": st(shard="1", src="m"),
        "c": st(shard="2", src="m"),
        "d": st(shard="0", src="m", misses="1"),
    }
    seen = {"m": seen_of("m", {0: {"a"}, 2: {"c"}}, expected=[0, 1, 2])}
    j = closure.Judge(prev, seen, ["m"], TODAY, ratio=0.4)
    assert j.plans["m"].mode == "partial"
    assert j.decide_row("a", "m", "").status == "active"
    assert (
        j.decide_row("d", "m", "").status == "closed"
    )  # シャード0は完走したのに居ない
    b = j.decide_row("b", "m", "")  # シャード1は欠け -> 保留
    assert (b.status, b.reason) == ("active", "shard_incomplete")


def test_retention_windows():
    assert closure.within_retention(st("active", last="2020-01-01"), TODAY)
    assert closure.within_retention(st("closed", closed="2025-10-21"), TODAY)
    assert not closure.within_retention(st("closed", closed="2025-10-19"), TODAY)
    assert not closure.within_retention(
        st("unknown", last="", first="2025-01-01"), TODAY
    )
    assert closure.within_retention(st("unknown", last="2026-09-01"), TODAY)


def test_state_roundtrip_and_header_check():
    state = {"https://x/a": st("closed", "not_seen", closed="2026-10-01", shard="3")}
    buf = io.StringIO()
    closure.write_state(buf, state)
    assert closure.read_state(buf.getvalue().splitlines(keepends=True)) == state
    with pytest.raises(ValueError):
        closure.read_state(["a,b\n"])


def test_migration_initial_state():
    rows = [
        {
            "job_url": "https://x/A",
            "source": "m",
            "first_seen_date": "2026-09-10",
            "listing_status": "unknown",
            "closed_detected_date": "",
        },
        {
            "job_url": "https://x/a",
            "source": "m",
            "first_seen_date": "2026-09-01",
            "listing_status": "closed",
            "closed_detected_date": "2026-10-02",
        },
        {
            "job_url": "https://x/b",
            "source": "m",
            "first_seen_date": "2026-09-01",
            "listing_status": "closed",
            "closed_detected_date": "",
        },
        {
            "job_url": "https://x/c",
            "source": "m",
            "first_seen_date": "2026-09-01",
            "listing_status": "active",
            "closed_detected_date": "",
        },
        {
            "job_url": "https://x/d",
            "source": "m",
            "first_seen_date": "2026-09-01",
            "listing_status": "unknown",
            "closed_detected_date": "",
        },
    ]
    seen = {"m": seen_of("m", {1: {"https://x/c"}})}
    s = migrate_state.build_initial_state(rows, seen, "2026-10-19", "2026-10-19")
    assert s["https://x/a"] == St(
        "m", "2026-09-01", "", "", "closed", "not_seen", "2026-10-02"
    )
    assert s["https://x/b"].closed_date == "2026-10-19"
    assert s["https://x/c"] == St(
        "m", "2026-09-01", "2026-10-19", "1", "active", "seen", ""
    )
    assert (
        s["https://x/d"].status == "unknown" and s["https://x/d"].reason == "migrated"
    )


# ---- L1 refresh ---------------------------------------------------------------


class _FakeS3:
    def __init__(self):
        self.put = {}

    def put_object(self, Bucket, Key, Body):
        self.put[Key] = Body


def _row(url, title="t", **kw):
    return {"company_name": "c", "job_title": title, "job_url": url, **kw}


def test_fingerprint_ignores_scraped_at_but_sees_content():
    a = _row("https://x/1", scraped_at="2026-10-01")
    assert jsl.content_fingerprint(a) == jsl.content_fingerprint(
        {**a, "scraped_at": "2026-10-20"}
    )
    assert jsl.content_fingerprint(a) != jsl.content_fingerprint(
        {**a, "valid_through": "2026-11-01"}
    )


def test_write_l1_refresh_appends_new_and_changed_only(monkeypatch):
    fake = _FakeS3()
    monkeypatch.setattr(jsl, "_s3", lambda: fake)
    monkeypatch.setattr(
        jsl, "_read_csv_s3", lambda key: (_ for _ in ()).throw(KeyError(key))
    )
    same, changed = _row("https://x/same"), _row("https://x/chg", title="old")
    monkeypatch.setattr(
        jsl,
        "known_fingerprints",
        lambda src: {
            "https://x/same": jsl.content_fingerprint(same),
            "https://x/chg": jsl.content_fingerprint(changed),
        },
    )
    rows = [same, _row("https://x/chg", title="new"), _row("https://x/new")]
    key, n = jsl.write_l1("src", rows, run_date="20261020", refresh=True)
    assert n == 2
    body = fake.put[key].decode("utf-8-sig")
    assert (
        "https://x/new" in body
        and "https://x/chg" in body
        and "https://x/same" not in body
    )


def test_valid_through_ignored_for_sources_where_it_is_stale():
    # 確認できた行の95%で valid_through が過去(更新されない値)の媒体では、valid_through で閉じない
    seen = {"m": seen_of("m", {0: {f"u{i}" for i in range(20)}})}
    prev = {"x": st(src="m", last="2026-10-19")}
    j = closure.Judge(prev, seen, ["m"], TODAY)
    acc = Counter()
    for i in range(20):
        j.vt_past_rate("m", f"u{i}", "2026-01-01" if i else "2027-01-01", acc)
    assert j.set_vt_reliability(acc) == {"m"}
    hold = closure.SourcePlan("m", "hold", "no_seen_file")
    assert (
        closure.decide("x", "m", prev["x"], None, hold, "2026-01-01", TODAY).status
        == "closed"
    )
    j.plans["m"] = hold
    assert j.decide_row("x", "m", "2026-01-01").status == "active"
