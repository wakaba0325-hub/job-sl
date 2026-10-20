"""一回限りの移行: 既存 job_master(+同日のseen_urls)から job_state の初期状態を作る。

    python -m job_sl.migrate_state --date 20261020                       # S3のjob_masterを読み、ローカルに書く(dry-run)
    python -m job_sl.migrate_state --date 20261020 --apply               # S3 master/job_state/20261020/ へ書込
    python -m job_sl.migrate_state --job-master ./jm.csv --date 20261020 # ローカルのjob_masterから(S3は読まない)

--date は移行元job_masterの日付(= job_state の日付)。移行後の最初の build_l3 はこの日付より
後の日に走らせる(build_l3 は「当日より前の最新」の job_state を前回状態として読むため)。

初期化の規則(job_urlの小文字・trimキーで重複を畳む。重複時は active > closed > unknown、
first_seen は最小、closed_detected_date は最小):
  - その日の seen_urls に居る → active / seen / last_seen_date=--date / last_seen_shard
  - job_master が closed → closed / not_seen / 検知日は元のまま(空なら --date)
  - それ以外(unknown、seenに居ないactive) → unknown / migrated
valid_through が過去のunknownは、移行後の最初の build_l3 で closed(valid_through_expired)になる。
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from typing import Dict, Iterable

from . import BUCKET, _s3, iter_lines_s3
from .build_l3 import SHARDED_SOURCES, load_seen
from .closure import St, SourceSeen, norm_key, write_state
from .l3 import CLOSURE_DETECTION_SOURCES

csv.field_size_limit(10**7)

_RANK = {"active": 2, "closed": 1}


def build_initial_state(
    rows: Iterable[dict],
    seen_by_source: Dict[str, SourceSeen],
    seen_date: str,
    state_date: str,
) -> Dict[str, St]:
    url_shard = {s: ss.url_shard() for s, ss in seen_by_source.items()}
    acc: Dict[str, list] = {}  # key -> [source, first, status, closed]
    for r in rows:
        k = norm_key(r.get("job_url", ""))
        if not k:
            continue
        src = r.get("source", "")
        first = r.get("first_seen_date", "") or state_date
        status = r.get("listing_status", "")
        closed = r.get("closed_detected_date", "")
        a = acc.get(k)
        if a is None:
            acc[k] = [src, first, status, closed]
            continue
        a[1] = min(a[1], first)
        if _RANK.get(status, 0) > _RANK.get(a[2], 0):
            a[2] = status
        if closed and (not a[3] or closed < a[3]):
            a[3] = closed
    out: Dict[str, St] = {}
    for k, (src, first, status, closed) in acc.items():
        sh = url_shard.get(src, {}).get(k)
        if sh is not None:
            out[k] = St(src, first, seen_date, str(sh), "active", "seen", "")
        elif status == "closed":
            out[k] = St(src, first, "", "", "closed", "not_seen", closed or state_date)
        else:
            out[k] = St(src, first, "", "", "unknown", "migrated", "")
    return out


def _iso(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="移行元job_masterの日付 YYYYMMDD")
    ap.add_argument("--job-master", help="ローカルのjob_master.csv(省略時はS3の該当日)")
    ap.add_argument("--out", default="job_state_migrated.csv", help="ローカル出力先")
    ap.add_argument(
        "--apply", action="store_true", help="S3 master/job_state/{date}/ へ書込"
    )
    a = ap.parse_args()
    if not re.fullmatch(r"\d{8}", a.date):
        sys.exit("--date は YYYYMMDD")

    seen = {}
    for src in CLOSURE_DETECTION_SOURCES:
        n = len(SHARDED_SOURCES.get(src, [None]))
        seen[src] = load_seen(src, n, a.date)
        print(
            f"  seen {src}: シャード{sorted(seen[src].present)} / {seen[src].total():,}件"
        )

    if a.job_master:
        f = open(a.job_master, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(f)
    else:
        reader = csv.DictReader(
            iter_lines_s3(f"master/job_master/{a.date}/job_master.csv")
        )
    state = build_initial_state(reader, seen, _iso(a.date), _iso(a.date))

    with open(a.out, "w", encoding="utf-8-sig", newline="") as w:
        write_state(w, state)
    st = {}
    for s in state.values():
        st[s.status] = st.get(s.status, 0) + 1
    print(f"状態 {len(state):,}件 {st} -> {a.out}")
    if not a.apply:
        print("[dry-run] S3へは書込まない")
        return
    key = f"master/job_state/{a.date}/job_state.csv"
    _s3().upload_file(a.out, BUCKET, key)
    print(f"[apply] s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
