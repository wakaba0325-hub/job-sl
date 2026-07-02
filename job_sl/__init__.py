"""job-sl: job_sources 共通土台(全ソースで同一の配管)。

層:
  L1  raw/job_sources/<source>/{YYYYMMDD}/<source>.csv      生データ・当日新規を追記(不変)
  L2  master/job_sources/<source>/{YYYYMMDD}/<source>.csv   job_urlキーでdedupしたソースmaster(最新日=全件)
  L3  master/job_master/{YYYYMMDD}/...                       全ソース統合+company_master突合は別リポ(本パッケージ外)

設計決定:
- キーは全ソース共通で job_url(求人詳細URLは媒体側で一意)。
- 各媒体の schema.org JobPosting 相当のフィールドを共通スキーマとして採用。
"""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config as _BotoConfig

REGION = "ap-northeast-1"
BUCKET = "emooove-data-lake"
JST = timezone(timedelta(hours=9))

COMMON_SCHEMA = [
    "source",
    "company_name",
    "job_title",
    "employment_type",
    "salary_min",
    "salary_max",
    "salary_currency",
    "location",
    "posted_date",
    "valid_through",
    "job_url",
    "description_snippet",
    "scraped_at",
]

_S3_CONFIG = _BotoConfig(
    region_name=REGION,
    read_timeout=900,
    connect_timeout=30,
    retries={"max_attempts": 5, "mode": "standard"},
)


def _s3():
    return boto3.client("s3", config=_S3_CONFIG)


def _today():
    return datetime.now(JST).strftime("%Y%m%d")


def key_for(row: dict):
    """L2 dedupキー: job_url。"""
    return (row.get("job_url") or "").strip().lower()


def to_common(source: str, r: dict) -> dict:
    return {
        "source": source,
        "company_name": (r.get("company_name") or "").strip(),
        "job_title": (r.get("job_title") or "").strip(),
        "employment_type": (r.get("employment_type") or "").strip(),
        "salary_min": (r.get("salary_min") or "").strip(),
        "salary_max": (r.get("salary_max") or "").strip(),
        "salary_currency": (r.get("salary_currency") or "").strip(),
        "location": (r.get("location") or "").strip(),
        "posted_date": (r.get("posted_date") or "").strip(),
        "valid_through": (r.get("valid_through") or "").strip(),
        "job_url": (r.get("job_url") or "").strip(),
        "description_snippet": (r.get("description_snippet") or "").strip()[:300],
        "scraped_at": (r.get("scraped_at") or "").strip(),
    }


# ---- L1 読み書き -----------------------------------------------------------


def _read_csv_s3(key: str) -> list[dict]:
    body = _s3().get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(body)))


def read_all_l1(source: str) -> list[dict]:
    """そのソースの全L1パーティション(=追記ログ)を union して返す。"""
    s3 = _s3()
    rows = []
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=BUCKET, Prefix=f"raw/job_sources/{source}/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(f"/{source}.csv") and "/_" not in o["Key"]:
                rows.extend(_read_csv_s3(o["Key"]))
    return rows


def write_l1(source: str, rows: list[dict], run_date: str | None = None):
    """生データを当日L1へ追記(既存全パーティションのjob_urlで重複除外)。"""
    seen = {key_for(to_common(source, r)) for r in read_all_l1(source)}
    new = [r for r in rows if key_for(to_common(source, r)) not in seen and key_for(r)]
    rd = run_date or _today()
    key = f"raw/job_sources/{source}/{rd}/{source}.csv"
    existing = []
    try:
        existing = _read_csv_s3(key)
    except Exception:
        pass
    cols = (
        list(existing[0].keys()) if existing else list(rows[0].keys()) if rows else []
    )
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(existing + new)
    _s3().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8-sig"))
    return key, len(new)


# ---- L2 ビルド -------------------------------------------------------------


def build_l2(source: str, run_date: str | None = None, write: bool = True):
    """L1全パーティションを union→正規化→job_urlでdedup(最新scraped_at採用)→L2へ。"""
    commons = [to_common(source, r) for r in read_all_l1(source)]
    commons = [c for c in commons if c["job_url"] and c["company_name"]]

    groups: dict = {}
    for c in commons:
        groups.setdefault(key_for(c), []).append(c)

    out = []
    for _, items in groups.items():
        items.sort(key=lambda c: c["scraped_at"])  # 古→新
        base = dict(items[-1])
        for col in COMMON_SCHEMA:
            if not base.get(col):
                for c in reversed(items):
                    if c.get(col):
                        base[col] = c[col]
                        break
        out.append(base)

    rd = run_date or _today()
    key = f"master/job_sources/{source}/{rd}/{source}.csv"
    if write:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=COMMON_SCHEMA, extrasaction="ignore")
        w.writeheader()
        w.writerows(out)
        _s3().put_object(
            Bucket=BUCKET, Key=key, Body=buf.getvalue().encode("utf-8-sig")
        )
    return out, key


# ---- 最新パーティション解決 -------------------------------------------------


def latest_key(prefix: str, fname: str) -> str | None:
    """`{prefix}{YYYYMMDD}/{fname}` を list_objects_v2 + 正規表現で最新解決。"""
    s3 = _s3()
    pat = re.compile(rf"^{re.escape(prefix)}(\d{{8}})/{re.escape(fname)}$")
    dates = []
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            m = pat.match(o["Key"])
            if m:
                dates.append(m.group(1))
    return f"{prefix}{max(dates)}/{fname}" if dates else None
