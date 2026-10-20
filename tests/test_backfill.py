import io
from datetime import date

import pytest

import job_sl
from job_sl import backfill, build_l3, closure, l3, rowcheck

TODAY = date(2026, 10, 20)
L2_HEADER = "job_url,company_name,location,valid_through,scraped_at\n"


class _Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def iter_lines(self, keepends=False):
        yield from io.BytesIO(self.data).readlines()


class FakeS3:
    def __init__(self, store, versioning="Enabled"):
        self.store = store
        self.versioning = versioning
        self.deleted = []

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.store[Key])}

    def get_paginator(self, name):
        store = self.store

        class P:
            def paginate(self, Bucket, Prefix):
                yield {
                    "Contents": [
                        {"Key": k, "Size": len(v)}
                        for k, v in sorted(store.items())
                        if k.startswith(Prefix)
                    ]
                }

        return P()

    def upload_file(self, path, Bucket, Key):
        with open(path, "rb") as f:
            self.store[Key] = f.read()

    def get_bucket_versioning(self, Bucket):
        return {"Status": self.versioning} if self.versioning else {}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.deleted.append(o["Key"])
            del self.store[o["Key"]]

    def copy(self, src, Bucket, Key):
        self.store[Key] = self.store[src["Key"]]


def seen_csv(urls):
    return ("job_url\n" + "".join(u + "\n" for u in urls)).encode()


def l2_csv(rows):
    return (L2_HEADER + "".join(",".join(r) + "\n" for r in rows)).encode("utf-8-sig")


@pytest.fixture
def fake(monkeypatch, tmp_path):
    store = {}
    f = FakeS3(store)
    for m in (job_sl, build_l3, backfill):
        monkeypatch.setattr(m, "_s3", lambda: f)
    monkeypatch.setattr(job_sl, "_today", lambda: "20261020")
    monkeypatch.setattr(backfill, "_today", lambda: "20261020")
    monkeypatch.setattr(l3, "CLOSURE_DETECTION_SOURCES", ["green"])
    monkeypatch.chdir(tmp_path)
    return f


# ---- rowcheck -------------------------------------------------------------------


@pytest.mark.parametrize(
    "row,issue",
    [
        (
            {
                "job_url": "https://a/1",
                "company_name": "株式会社A",
                "scraped_at": "2026-10-01T00:00:00+09:00",
            },
            None,
        ),
        ({"job_url": " ", "company_name": "A"}, "no_url"),
        ({"job_url": "javascript:void(0)", "company_name": "A"}, "bad_url"),
        ({"job_url": "https://a/1 2", "company_name": "A"}, "bad_url"),
        ({"job_url": "https://a/1", "company_name": "  "}, "no_company"),
        (
            {
                "job_url": "https://a/1",
                "company_name": "A",
                "scraped_at": "2027-01-01T00:00:00",
            },
            "future_scraped_at",
        ),
        (
            {"job_url": "https://a/1", "company_name": "A", "scraped_at": "garbage-xx"},
            "bad_scraped_at",
        ),
        ({"job_url": "https://a/1", "company_name": "A", "scraped_at": ""}, None),
    ],
)
def test_row_issue(row, issue):
    assert rowcheck.row_issue(row, TODAY) == issue


def test_normalize_valid_through_drops_placeholders():
    n = rowcheck.normalize_valid_through
    assert (
        n("2046-08-17", "2026-08-23T00:00:00+09:00") == ""
    )  # mynavi_pharma: 掲載日+20年
    assert n("9999-12-31", "2026-08-23") == ""
    assert n("2026-11-30", "2026-10-20T01:00:00+09:00") == "2026-11-30"
    assert n("", "2026-10-20") == ""
    assert n("来月末", "2026-10-20") == "来月末"


# ---- universe -------------------------------------------------------------------


def test_universe_keeps_earliest_first_seen_and_newest_row(fake):
    fake.store["master/job_sources/g0/20261020/g0.csv"] = l2_csv(
        [
            ("https://g/A", "社A", "", "", "2026-10-10T00:00:00+09:00"),
            ("https://g/bad one", "社X", "", "", "2026-10-10T00:00:00+09:00"),
            ("https://g/c", "", "", "", "2026-10-10T00:00:00+09:00"),
        ]
    )
    fake.store["master/job_sources/g1/20261020/g1.csv"] = l2_csv(
        [("https://g/a ", "社A", "", "2026-11-01", "2026-09-01T00:00:00+09:00")]
    )
    files = [
        ("geekly", "g0", "master/job_sources/g0/20261020/g0.csv"),
        ("geekly", "g1", "master/job_sources/g1/20261020/g1.csv"),
    ]
    uni, st = backfill.load_universe(files, TODAY)
    assert list(uni) == ["https://g/a"]
    src, vt, first, sa = uni["https://g/a"]
    assert first == "2026-09-01"  # シャード間で入れ替わったURLの初回取得日を失わない
    assert sa.startswith("2026-10-10") and vt == ""  # 内容は新しい方の行
    assert (st["bad_url"], st["no_company"], st["dup_rows"]) == (1, 1, 1)


# ---- replay ---------------------------------------------------------------------


def _replay_env(fake):
    fillers = [f"https://g/h{i}" for i in range(6)]
    rows = [
        ("https://g/a", "社A", "", "", "2026-10-01T00:00:00+09:00"),  # 毎日確認
        (
            "https://g/b",
            "社A",
            "",
            "",
            "2026-10-01T00:00:00+09:00",
        ),  # 10/18のみ確認→10/19から消える
        (
            "https://g/c",
            "社A",
            "",
            "",
            "2026-10-01T00:00:00+09:00",
        ),  # 一度も確認できない
        ("https://g/d", "社A", "", "", "2026-10-19T09:00:00+09:00"),  # 10/19に初出
        (
            "https://g/e",
            "社A",
            "",
            "2025-01-01",
            "2026-10-01T00:00:00+09:00",
        ),  # 期限が1年超前
        (
            "https://g/f",
            "社A",
            "",
            "",
            "2026-10-20T09:00:00+09:00",
        ),  # 10/20に初出・未確認
        (
            "https://g/future",
            "社A",
            "",
            "",
            "2027-05-01T00:00:00+09:00",
        ),  # 未来日付は除外
    ] + [(u, "社A", "", "", "2026-10-01T00:00:00+09:00") for u in fillers]
    fake.store["master/job_sources/green/20261020/green.csv"] = l2_csv(rows)
    fake.store["raw/job_sources/green/20261018/green_seen_urls.csv"] = seen_csv(
        ["https://g/a", "https://g/b"] + fillers
    )
    fake.store["raw/job_sources/green/20261019/green_seen_urls.csv"] = seen_csv(
        ["https://g/a", "https://g/d"] + fillers
    )
    fake.store["raw/job_sources/green/20261020/green_seen_urls.csv"] = seen_csv(
        ["https://g/a", "https://g/d"] + fillers
    )
    # seenが無い日・別媒体のseenは日付判定に影響しない
    fake.store["raw/job_sources/green/20261017/green.csv"] = b"x"


def test_replay_state_restores_history(fake):
    _replay_env(fake)
    assert backfill.replay_state("20261020", apply=True, today_s="20261020") == 0
    state = closure.read_state(
        fake.store["master/job_state/20261020/job_state.csv"]
        .decode("utf-8-sig")
        .splitlines(True)
    )
    a, b, c, d, f = (state[f"https://g/{x}"] for x in "abcdf")
    assert (a.status, a.reason, a.first_seen, a.last_seen) == (
        "active",
        "seen",
        "2026-10-01",
        "2026-10-20",
    )
    # 最後に確認できた10/18の翌日が終了検知日。連続2回未確認で閉じる(10/20)が、日付は根拠のある10/19
    assert (b.status, b.reason, b.closed_date, b.last_seen) == (
        "closed",
        "not_seen",
        "2026-10-19",
        "2026-10-18",
    )
    # 一度も確認できないまま閉じた行は推定として区別する
    assert (c.status, c.reason, c.last_seen) == ("closed", "backfill_never_seen", "")
    assert (d.status, d.first_seen, d.last_seen) == (
        "active",
        "2026-10-19",
        "2026-10-20",
    )
    assert f.status == "unknown" and f.first_seen == "2026-10-20"
    assert "https://g/e" not in state  # 終了から1年超は状態から外れる
    assert "https://g/future" not in state  # 未来日付の行は除外
    assert "https://g/c" in state


def test_replay_dry_run_writes_no_s3(fake):
    _replay_env(fake)
    before = dict(fake.store)
    assert backfill.replay_state("20261020", apply=False, today_s="20261020") == 0
    assert fake.store == before


def test_replay_refuses_to_overwrite_existing_state(fake):
    _replay_env(fake)
    fake.store["master/job_state/20261020/job_state.csv"] = b"keep"
    assert backfill.replay_state("20261020", apply=True, today_s="20261020") == 1
    assert fake.store["master/job_state/20261020/job_state.csv"] == b"keep"


def test_replay_skips_days_without_seen_and_holds_unsupported_sources(fake):
    _replay_env(fake)
    uni, _ = backfill.load_universe(build_l3._l2_files(), TODAY)
    final, summary = backfill.replay(
        uni,
        ["20261018", "20261020"],
        "20261020",
        build_l3.load_seen,
        log=lambda *_: None,
    )
    # seenの無い日(10/19)は飛ばす=その日は判定しないので、bは1回しか未確認にならず閉じない。cは10/18と10/20の2回で閉じる
    assert final["https://g/b"].reason == "miss_pending"
    assert final["https://g/c"].status == "closed"
    assert summary["days"] == 2


def test_replay_next_build_accepts_state(fake):
    """復元した job_state を build_l3 が前回状態として読める(列・ヘッダの互換)。"""
    _replay_env(fake)
    backfill.replay_state("20261020", apply=True, today_s="20261020")
    prev, date_ = build_l3._load_prev_state("20261021")
    assert date_ == "20261020" and len(prev) > 5


# ---- rebuild-l2 -----------------------------------------------------------------


def _l1(url, company="社A", sa="2026-10-05T00:00:00+09:00"):
    return f"source,company_name,job_title,job_url,scraped_at\nx,{company},t,{url},{sa}\n".encode(
        "utf-8-sig"
    )


def test_rebuild_l2_detects_frozen_source_and_writes_new_key(fake):
    fake.store["raw/job_sources/x/20261001/x.csv"] = _l1("https://x/1")
    fake.store["raw/job_sources/x/20261019/x.csv"] = _l1("https://x/2")
    fake.store["master/job_sources/x/20261001/x.csv"] = b"old"
    fake.store["raw/job_sources/ok/20261019/ok.csv"] = _l1("https://ok/1")
    fake.store["master/job_sources/ok/20261019/ok.csv"] = b"new"
    stale = backfill.stale_l2(
        backfill.list_objects("raw/job_sources/"),
        backfill.list_objects("master/job_sources/"),
    )
    assert stale == [("x", "20261019", "20261001")]
    assert backfill.rebuild_l2(["x"], apply=False, today_s="20261020") == []
    assert "master/job_sources/x/20261020/x.csv" not in fake.store
    assert backfill.rebuild_l2(["x", "ok"], apply=True, today_s="20261020") == ["x"]
    out = fake.store["master/job_sources/x/20261020/x.csv"].decode("utf-8-sig")
    assert "https://x/1" in out and "https://x/2" in out
    assert (
        fake.store["master/job_sources/x/20261001/x.csv"] == b"old"
    )  # 旧世代はそのまま


def test_rebuild_l2_caps_sources_per_run(fake):
    for s in "xyz":
        fake.store[f"raw/job_sources/{s}/20261019/{s}.csv"] = _l1(f"https://{s}/1")
        fake.store[f"master/job_sources/{s}/20261001/{s}.csv"] = b"old"
    done = backfill.rebuild_l2(
        ["x", "y", "z"], apply=True, today_s="20261020", max_sources=2
    )
    assert done == ["x", "y"]
    assert "master/job_sources/z/20261020/z.csv" not in fake.store


# ---- prune ----------------------------------------------------------------------


def _gens(fake, prefix, fname, dates, size=10):
    for d in dates:
        fake.store[f"{prefix}{d}/{fname}"] = b"x" * size


DATES = [f"202610{d:02d}" for d in range(1, 11)]  # 10世代


def test_prune_dry_run_changes_nothing(fake):
    _gens(fake, "master/job_master/", "job_master.csv", DATES)
    before = dict(fake.store)
    assert backfill.prune(apply=False, today_s="20261020") == 0
    assert fake.store == before and not fake.deleted


def test_prune_keeps_newest_n_and_respects_limit(fake):
    _gens(fake, "master/job_master/", "job_master.csv", DATES)
    _gens(fake, "master/job_sources/green/", "green.csv", DATES)
    _gens(fake, "master/job_sources/_odd/", "x.csv", ["20261001"])
    backfill.prune(apply=True, max_objects=2, today_s="20261020", keep_jm=7, keep_l2=14)
    assert fake.deleted == [
        "master/job_master/20261001/job_master.csv",
        "master/job_master/20261002/job_master.csv",
    ]  # 古い順・job_master優先・上限2
    backfill.prune(
        apply=True, max_objects=10, today_s="20261020", keep_jm=7, keep_l2=14
    )
    assert fake.deleted[2:] == ["master/job_master/20261003/job_master.csv"]
    assert "master/job_master/20261004/job_master.csv" in fake.store
    assert (
        len([k for k in fake.store if "job_sources/green" in k]) == 10
    )  # L2は14世代以内なので残す


def test_prune_l2_keep_and_protect(fake):
    _gens(fake, "master/job_sources/green/", "green.csv", DATES)
    backfill.prune(
        apply=True, today_s="20261020", keep_l2=4, protect=["20261001"], keep_jm=7
    )
    assert "master/job_sources/green/20261001/green.csv" in fake.store  # protect
    assert "master/job_sources/green/20261002/green.csv" not in fake.store
    assert "master/job_sources/green/20261007/green.csv" in fake.store
    assert len([k for k in fake.store if "green/" in k]) == 5  # 最新4 + protect


def test_prune_refuses_without_versioning(fake):
    fake.versioning = "Suspended"
    _gens(fake, "master/job_master/", "job_master.csv", DATES)
    assert backfill.prune(apply=True, today_s="20261020") == 1
    assert not fake.deleted


def test_prune_never_touches_empty_latest_future_or_other_keys(fake):
    _gens(fake, "master/job_master/", "job_master.csv", DATES)
    fake.store["master/job_master/20261010/job_master.csv"] = b""  # 最新が空
    _gens(
        fake,
        "master/job_sources/green/",
        "green.csv",
        ["20261001", "20261002", "29990101"],
    )
    fake.store["master/job_sources/green/_backup/20260101/green.csv"] = b"b"
    fake.store["master/job_sources/green/20261001/green_seen_urls.csv"] = b"s"
    backfill.prune(apply=True, today_s="20261020", keep_l2=1, keep_jm=7)
    assert not any(k.startswith("master/job_master/") for k in fake.deleted)
    assert fake.deleted == ["master/job_sources/green/20261001/green.csv"]
    assert (
        "master/job_sources/green/29990101/green.csv" in fake.store
    )  # 未来日付は触らない
    assert "master/job_sources/green/_backup/20260101/green.csv" in fake.store


def test_prune_backup_mode_copies_before_delete(fake):
    _gens(fake, "master/job_master/", "job_master.csv", DATES)
    backfill.prune(apply=True, mode="backup", max_objects=1, today_s="20261020")
    assert "master/job_master/_backup/20261001/job_master.csv" in fake.store
    assert "master/job_master/20261001/job_master.csv" not in fake.store


def test_plan_prune_reports_future_generation(fake):
    _gens(fake, "master/job_master/", "job_master.csv", DATES + ["20271231"])
    victims, notes = backfill.plan_prune(
        backfill.list_objects("master/job_master/"), "20261020"
    )
    assert any("20271231" in n for n in notes)
    assert [v[3] for v in victims] == ["20261001", "20261002", "20261003"]


# ---- audit ----------------------------------------------------------------------


def test_audit_counts_anomalies_and_folders(fake, tmp_path):
    fake.store["master/job_sources/green/20261020/green.csv"] = l2_csv(
        [
            ("https://g/1", "社A", "", "", "2026-10-01T00:00:00+09:00"),
            ("https://g/1", "社A", "", "", "2026-10-02T00:00:00+09:00"),
            ("https://g/2", "", "", "", "2026-10-01T00:00:00+09:00"),
            ("ftp://x", "社A", "", "", "2026-10-01T00:00:00+09:00"),
        ]
    )
    fake.store["raw/job_sources/green/20991231/green.csv"] = b"x"
    rep = backfill.audit(today_s="20261020", out_path=str(tmp_path / "a.json"))
    c = rep["rows_by_source"]["green"]
    assert (c["rows"], c["no_company"], c["bad_url"], c["dup_rows"]) == (4, 1, 1, 1)
    assert rep["unique_urls"] == 1
    assert any("20991231" in x for x in rep["folder_anomalies"])
    assert rep["stale_l2"][0]["source"] == "green"
