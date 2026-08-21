"""build_l3: 全ソースのL2 + company_master + 既存job_masterを統合しjob_masterを再生成する。

入力:
  - L2 各ソース最新パーティション  master/job_sources/<source>/{最新}/<source>.csv
  - company_master 最新            master/company_master/{最新}/company_master.csv
  - 既存 job_master 最新(あれば)    master/job_master/{最新}/job_master.csv
      (job_urlごとの first_seen_date / listing_status / closed_detected_date を引き継ぐため)
  - 当日の seen_urls  raw/job_sources/<source>/{当日}/<source>_seen_urls.csv
      (l3.CLOSURE_DETECTION_SOURCES対象ソースのみ。各コレクタがサイト全体を毎日フル巡回する中で
      既に計算済みの「本日確認できた全URL」をそのまま書き出したもの。jobmedley/mynaviは
      シャード実行時 `<source>_seen_urls_shard{N}.csv` に分かれるため全シャード分をunionする)

処理:
  1. 全ソースL2をunion
  2. company_master突合で法人番号付与(l3.match_houjin)
  3. job_urlで既存job_masterと突合し first_seen_date を継承、無ければ当日=新規(is_new_today=1)
  4. 掲載終了検知: l3.CLOSURE_DETECTION_SOURCES対象ソースは、当日のseen_urlsに無いjob_urlを
     listing_status="closed"とする(初検知日はclosed_detected_dateに記録し、以降は変えない)。
     安全装置: 当日確認できたURL件数が前日job_master内の同ソース件数の
     l3.SAFETY_THRESHOLD_RATIO(既定70%)を下回った場合、クロール不完全の疑いとして
     当日はそのソースの掲載終了判定を全件スキップし、listing_status="unknown"とする。
     対象外ソースはlisting_statusを常に"unknown"とする。
  5. master/job_master/{当日}/job_master.csv へ書込(スナップショット型・全件洗い替え)

既定は dry-run。--apply で書込。
"""

from __future__ import annotations

import argparse
import csv
import io
import re
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
    "levtech_career",
    "levtech_freelance",
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
    """既存job_masterから job_url→{first_seen_date, listing_status, closed_detected_date}
    のメタ情報と、ソース別の行数(安全装置の前日ベースライン)を返す。"""
    key = latest_key(JM_PREFIX, JM_FILE)
    if not key:
        print("  既存job_master: なし(初回ビルド)")
        return {}, Counter()
    rows = _read_csv_s3(key)
    print(f"  既存job_master: {key}  ({len(rows):,}行)")
    prev_meta = {}
    prev_source_counts = Counter()
    for r in rows:
        job_url = r.get("job_url")
        if not job_url:
            continue
        prev_meta[job_url] = {
            "first_seen_date": r.get("first_seen_date", ""),
            "listing_status": r.get("listing_status", ""),
            "closed_detected_date": r.get("closed_detected_date", ""),
        }
        prev_source_counts[r.get("source", "")] += 1
    return prev_meta, prev_source_counts


def _load_seen_urls(source: str, today: str) -> tuple[set[str], list[str]]:
    """当日の `{source}_seen_urls.csv`(シャード分割時は `{source}_seen_urls_shard{N}.csv` 全て)
    を読み込みunionしたjob_url集合を返す。戻り値は (job_url集合, 読んだキー一覧)。
    該当ファイルが1つも無ければ空集合(=安全装置が自動的に発火して当日の掲載終了判定が
    スキップされる)。"""
    prefix = f"raw/job_sources/{source}/{today}/"
    pat = re.compile(
        rf"^{re.escape(prefix)}{re.escape(source)}_seen_urls(_shard\d+)?\.csv$"
    )
    s3 = _s3()
    keys = []
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            if pat.match(o["Key"]):
                keys.append(o["Key"])
    urls: set[str] = set()
    for k in keys:
        try:
            rows = _read_csv_s3(k)
        except Exception:
            continue
        for r in rows:
            u = (r.get("job_url") or "").strip()
            if u:
                urls.add(u)
    return urls, keys


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

    prev_meta, prev_source_counts = _load_existing_job_master()

    print(f"\n=== 掲載終了検知: 当日seen_urls読込・安全装置判定 ===")
    seen_urls_by_source: dict[str, set[str]] = {}
    safety_ok_by_source: dict[str, bool] = {}
    for src in l3.CLOSURE_DETECTION_SOURCES:
        urls, keys = _load_seen_urls(src, today)
        seen_urls_by_source[src] = urls
        prev_count = prev_source_counts.get(src, 0)
        seen_count = len(urls)
        if prev_count == 0:
            ok, ratio_desc = True, "前日ベースライン無し(初回扱い)"
        else:
            ratio = seen_count / prev_count
            ok = ratio >= l3.SAFETY_THRESHOLD_RATIO
            ratio_desc = f"前日比{ratio:.1%}(閾値{l3.SAFETY_THRESHOLD_RATIO:.0%})"
        safety_ok_by_source[src] = ok
        print(
            f"  {src}: seen_urlsファイル{len(keys)}件 / 本日確認{seen_count:,}件 / "
            f"前日job_master{prev_count:,}件 / {ratio_desc} "
            f"[{'OK' if ok else '安全装置発火'}]"
        )
        if not ok:
            print(
                f"    [WARNING] {src}: 本日確認できたURL件数が閾値未満に急減。"
                f"クロール不完全の疑いがあるため、本日は{src}の掲載終了判定を全件スキップし"
                f"listing_status=unknownとします。"
            )

    out = []
    match_stats = Counter()
    method_stats = Counter()
    listing_status_stats: Counter = Counter()
    for r in all_rows:
        houjin, ambiguous, method = l3.match_houjin(
            r.get("company_name", ""), cm_index, location=r.get("location", "")
        )
        match_stats[
            "matched" if houjin else ("ambiguous" if ambiguous else "unmatched")
        ] += 1
        method_stats[method] += 1
        job_url = r.get("job_url", "")
        prev = prev_meta.get(job_url, {})
        first_seen = prev.get("first_seen_date") or today_iso
        is_new = "1" if job_url not in prev_meta else "0"

        src = r.get("source", "")
        if src in l3.CLOSURE_DETECTION_SOURCES and safety_ok_by_source.get(src):
            if job_url in seen_urls_by_source.get(src, set()):
                listing_status = "active"
                closed_detected_date = ""
            else:
                listing_status = "closed"
                closed_detected_date = prev.get("closed_detected_date") or today_iso
        elif src in l3.CLOSURE_DETECTION_SOURCES:
            # 安全装置発火中: 判定は行わず、closed_detected_dateのみ過去の検知記録を維持する
            listing_status = "unknown"
            closed_detected_date = prev.get("closed_detected_date", "")
        else:
            listing_status = "unknown"
            closed_detected_date = ""
        listing_status_stats[(src, listing_status)] += 1

        row = {k: r.get(k, "") for k in l3.JOB_MASTER_SCHEMA}
        row["houjin_bangou"] = houjin
        row["is_ambiguous_company"] = "1" if ambiguous else ""
        row["match_method"] = method
        row["first_seen_date"] = first_seen
        row["is_new_today"] = is_new
        row["listing_status"] = listing_status
        row["closed_detected_date"] = closed_detected_date
        out.append(row)

    print(f"\n=== company_master突合 ===")
    print(
        f"  マッチ: {match_stats['matched']:,} / 同名複数: {match_stats['ambiguous']:,} / 未マッチ: {match_stats['unmatched']:,}"
    )
    print(f"  内訳: {dict(method_stats)}")
    n_new = sum(1 for r in out if r["is_new_today"] == "1")
    print(f"\n=== 新規掲載検知 ===")
    print(f"  本日新規(is_new_today=1): {n_new:,} / 既存: {len(out) - n_new:,}")

    print(f"\n=== 掲載終了検知 結果(source別 listing_status) ===")
    by_source: dict[str, dict[str, int]] = {}
    for (src, status), n in listing_status_stats.items():
        by_source.setdefault(src, {})[status] = n
    for src in sorted(by_source):
        parts = ", ".join(f"{k}={v:,}" for k, v in sorted(by_source[src].items()))
        print(f"  {src}: {parts}")

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
