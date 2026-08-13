"""build_l3: 全ソースのL2 + company_master + 既存job_masterを統合しjob_masterを再生成する。

入力:
  - L2 各ソース最新パーティション  master/job_sources/<source>/{最新}/<source>.csv
  - company_master 最新            master/company_master/{最新}/company_master.csv
  - 既存 job_master 最新(あれば)    master/job_master/{最新}/job_master.csv
      (job_urlごとの first_seen_date を引き継ぐため)

処理:
  1. 全ソースL2をunion
  2. company_master突合で法人番号付与(l3.match_houjin)
  3. job_urlで既存job_masterと突合し first_seen_date を継承、無ければ当日=新規(is_new_today=1)
  4. master/job_master/{当日}/job_master.csv へ書込(スナップショット型・全件洗い替え)

既定は dry-run。--apply で書込。
"""

from __future__ import annotations

import argparse
import csv
import io
from collections import Counter

from . import BUCKET, _read_csv_s3, _s3, _today, latest_key
from . import l3

csv.field_size_limit(10**7)

SOURCES = [
    "green",
    "rikunabi",
    "wantedly",
    "bizreach",
    "type",
    "youtrust",
    "ambi",
    "paiza",
    "jobmedley",
]
# mynaviは並列書込の競合回避のためシャード別ソース名(mynavi0..6)で書かれている。
# ここで統合し、job_masterの表示上は source="mynavi" にまとめる。
SHARDED_SOURCES = {"mynavi": [f"mynavi{i}" for i in range(7)]}
L2_PREFIX = "master/job_sources/"
CM_PREFIX = "master/company_master/"
CM_FILE = "company_master.csv"
JM_PREFIX = "master/job_master/"
JM_FILE = "job_master.csv"


def _load_company_master():
    key = latest_key(CM_PREFIX, CM_FILE)
    if not key:
        raise SystemExit("company_master が見つかりません")
    body = (
        _s3()
        .get_object(Bucket=BUCKET, Key=key)["Body"]
        .read()
        .decode("utf-8-sig", errors="replace")
    )
    print(f"  company_master: {key}")
    return csv.DictReader(io.StringIO(body))


def _load_existing_job_master():
    key = latest_key(JM_PREFIX, JM_FILE)
    if not key:
        print("  既存job_master: なし(初回ビルド)")
        return {}
    rows = _read_csv_s3(key)
    print(f"  既存job_master: {key}  ({len(rows):,}行)")
    return {
        r["job_url"]: r.get("first_seen_date", "") for r in rows if r.get("job_url")
    }


def build(apply: bool):
    today = _today()
    today_iso = f"{today[:4]}-{today[4:6]}-{today[6:]}"

    all_rows = []
    for src in SOURCES:
        key = latest_key(f"{L2_PREFIX}{src}/", f"{src}.csv")
        if not key:
            print(f"  L2 {src}: なし")
            continue
        rows = _read_csv_s3(key)
        for r in rows:
            r["source"] = src
        all_rows.extend(rows)
        print(f"  L2 {src}: {key}  {len(rows):,}行")

    for display_src, shard_srcs in SHARDED_SOURCES.items():
        n_total = 0
        for shard_src in shard_srcs:
            key = latest_key(f"{L2_PREFIX}{shard_src}/", f"{shard_src}.csv")
            if not key:
                continue
            rows = _read_csv_s3(key)
            for r in rows:
                r["source"] = display_src
            all_rows.extend(rows)
            n_total += len(rows)
        print(f"  L2 {display_src}(shard統合): {n_total:,}行")

    print(f"\n全ソース合計: {len(all_rows):,}行")

    cm_index = l3.index_company_master(_load_company_master())
    print(f"company_master索引: {len(cm_index):,}社(正規化名ユニーク)")

    prev_first_seen = _load_existing_job_master()

    out = []
    match_stats = Counter()
    method_stats = Counter()
    for r in all_rows:
        houjin, ambiguous, method = l3.match_houjin(
            r.get("company_name", ""), cm_index, location=r.get("location", "")
        )
        match_stats[
            "matched" if houjin else ("ambiguous" if ambiguous else "unmatched")
        ] += 1
        method_stats[method] += 1
        job_url = r.get("job_url", "")
        first_seen = prev_first_seen.get(job_url) or today_iso
        is_new = "1" if job_url not in prev_first_seen else "0"
        row = {k: r.get(k, "") for k in l3.JOB_MASTER_SCHEMA}
        row["houjin_bangou"] = houjin
        row["is_ambiguous_company"] = "1" if ambiguous else ""
        row["match_method"] = method
        row["first_seen_date"] = first_seen
        row["is_new_today"] = is_new
        out.append(row)

    print(f"\n=== company_master突合 ===")
    print(
        f"  マッチ: {match_stats['matched']:,} / 同名複数: {match_stats['ambiguous']:,} / 未マッチ: {match_stats['unmatched']:,}"
    )
    print(f"  内訳: {dict(method_stats)}")
    n_new = sum(1 for r in out if r["is_new_today"] == "1")
    print(f"\n=== 新規掲載検知 ===")
    print(f"  本日新規(is_new_today=1): {n_new:,} / 既存: {len(out) - n_new:,}")

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=l3.JOB_MASTER_SCHEMA, extrasaction="ignore")
    w.writeheader()
    w.writerows(out)
    data = buf.getvalue().encode("utf-8-sig")

    if not apply:
        with open("job_master_dryrun.csv", "wb") as f:
            f.write(data)
        print("\n[dry-run] ローカル job_master_dryrun.csv に出力。S3へは書込まない。")
        return

    out_key = f"{JM_PREFIX}{today}/{JM_FILE}"
    _s3().put_object(Bucket=BUCKET, Key=out_key, Body=data)
    print(f"\n[apply] 書込完了: s3://{BUCKET}/{out_key}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="S3へ書込(既定はdry-run)")
    a = ap.parse_args()
    build(a.apply)


if __name__ == "__main__":
    main()
