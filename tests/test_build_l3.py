import csv
import io

import pytest

import job_sl
from job_sl import build_l3, closure, l3

L2_HEADER = "job_url,company_name,location,valid_through,scraped_at\n"


class _Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def iter_lines(self, keepends=False):
        yield from io.BytesIO(self.data).readlines()


class FakeS3:
    def __init__(self, store):
        self.store = store

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


def state_csv(rows):
    buf = io.StringIO()
    closure.write_state(buf, rows)
    return buf.getvalue().encode()


def seen_csv(urls):
    return ("job_url\n" + "".join(u + "\n" for u in urls)).encode()


@pytest.fixture
def env(monkeypatch, tmp_path):
    store = {
        "master/company_master/20260101/company_master.csv": "商号,法人番号,都道府県,処理区分\n株式会社C,1,東京都,01\n".encode(),
        "master/job_sources/green/20261020/green.csv": (
            L2_HEADER
            + "https://g/a,株式会社C,東京都,,2026-10-01\n"
            + "https://g/b,株式会社C,東京都,,2026-10-01\n"
            + "https://g/c,株式会社C,東京都,,2026-10-01\n"
            + "https://g/d,株式会社C,東京都,,2026-10-20\n"
            + "https://g/ancient,株式会社C,東京都,,2024-01-01\n"
        ).encode("utf-8-sig"),
        "master/job_state/20261019/job_state.csv": state_csv(
            {
                "https://g/a": closure.St(
                    "green", "2026-09-01", "2026-10-19", "0", "active", "seen", ""
                ),
                "https://g/b": closure.St(
                    "green", "2026-09-01", "2026-10-19", "0", "active", "seen", "", "1"
                ),
                "https://g/c": closure.St(
                    "green",
                    "2026-09-01",
                    "2026-09-30",
                    "0",
                    "closed",
                    "not_seen",
                    "2026-10-01",
                ),
                "https://g/ancient": closure.St(
                    "green", "2024-01-01", "", "", "closed", "not_seen", "2025-01-01"
                ),
            }
        ),
        "raw/job_sources/green/20261020/green_seen_urls.csv": seen_csv(
            ["https://G/a", "https://g/d"]
        ),
    }
    fake = FakeS3(store)
    monkeypatch.setattr(job_sl, "_s3", lambda: fake)
    monkeypatch.setattr(build_l3, "_s3", lambda: fake)
    monkeypatch.setattr(l3, "CLOSURE_DETECTION_SOURCES", ["green", "geekly"])
    monkeypatch.chdir(tmp_path)
    return store


def read_rows(store, key):
    return {
        r["job_url"]: r
        for r in csv.DictReader(io.StringIO(store[key].decode("utf-8-sig")))
    }


def test_build_transitions_and_outputs(env):
    code = build_l3.build(apply=True, today_s="20261020")
    rows = read_rows(env, "master/job_master/20261020/job_master.csv")
    assert set(rows) == {
        "https://g/a",
        "https://g/b",
        "https://g/c",
        "https://g/d",
    }  # 1年超のclosedは落ちる
    a, b, c, d = (rows[f"https://g/{x}"] for x in "abcd")
    assert (a["listing_status"], a["status_reason"], a["last_seen_date"]) == (
        "active",
        "seen",
        "2026-10-20",
    )
    assert (b["listing_status"], b["status_reason"], b["closed_detected_date"]) == (
        "closed",
        "not_seen",
        "2026-10-20",
    )
    assert (c["listing_status"], c["closed_detected_date"]) == ("closed", "2026-10-01")
    assert (d["listing_status"], d["is_new_today"], d["first_seen_date"]) == (
        "active",
        "1",
        "2026-10-20",
    )
    assert a["is_new_today"] == "0" and a["houjin_bangou"] == "1"
    state = closure.read_state(
        env["master/job_state/20261020/job_state.csv"]
        .decode("utf-8-sig")
        .splitlines(True)
    )
    assert set(state) == set(rows) and state["https://g/b"].status == "closed"
    assert code == 2  # geekly のseenが無い(想定外の保留) -> 書込済み・exit 2


def test_build_holds_when_source_did_not_finish(env):
    del env["raw/job_sources/green/20261020/green_seen_urls.csv"]
    build_l3.build(apply=True, today_s="20261020")
    rows = read_rows(env, "master/job_master/20261020/job_master.csv")
    assert (
        rows["https://g/b"]["listing_status"] == "active"
    )  # 巡回が完走しない日は閉じない
    assert rows["https://g/b"]["status_reason"] == "no_seen_file"


def test_build_refuses_without_previous_state(env):
    del env["master/job_state/20261019/job_state.csv"]
    assert build_l3.build(apply=True, today_s="20261020") == 1
    assert "master/job_master/20261020/job_master.csv" not in env


def test_build_refuses_when_l2_source_vanishes(env):
    state = closure.read_state(
        env["master/job_state/20261019/job_state.csv"]
        .decode("utf-8-sig")
        .splitlines(True)
    )
    for i in range(1200):
        state[f"https://z/{i}"] = closure.St(
            "zeta", "2026-09-01", "2026-10-19", "", "active", "seen", ""
        )
    env["master/job_state/20261019/job_state.csv"] = state_csv(state)
    assert build_l3.build(apply=True, today_s="20261020") == 1
    assert "master/job_state/20261020/job_state.csv" not in env


def test_dry_run_writes_nothing_to_s3(env):
    before = dict(env)
    build_l3.build(apply=False, today_s="20261020")
    assert env == before
