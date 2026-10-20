"""build_l3: 全ソースのL2 + company_master + 前回 job_state を統合し job_master と job_state を再生成する。

入力:
  - L2 各ソース最新パーティション  master/job_sources/<source>/{最新}/<source>.csv
  - company_master 最新            master/company_master/{最新}/company_master.csv
  - 前回 job_state                 master/job_state/{当日より前の最新}/job_state.csv
      (job_urlごとの first/last_seen・listing_status・closed_detected_date。同日再実行でも当日分を
      前回と取り違えない。job_master(十数GB)を前回状態として読まない)
  - 当日の seen_urls  raw/job_sources/<source>/{当日}/<source>_seen_urls[_shard{N}].csv
      (l3.CLOSURE_DETECTION_SOURCES対象。ファイルがある=その巡回が完走した。シャード媒体は
      SHARDED_SOURCES の本数ぶん揃って初めて媒体全体の判定をする。詳細は closure.py)

処理:
  1. 全ソースL2をjob_urlの小文字・trimキーで重複排除(シャード間重複。scraped_atが新しい行、同値なら情報が多い行を採用)。
     URL不正・社名空・未来日付のscraped_atの行は除外(rowcheck)。取得日から2年超先のvalid_throughは仮値として空にする
  2. closure.Judge で当日の状態(active/closed/unknown + status_reason)を決める
  3. 保持窓: closedは検知日から365日、unknownは最後の確認/初出から365日を過ぎたら出力しない
  4. company_master突合で法人番号付与(matching.match_houjin。match_confidence/is_staffing_agent/is_anonymousも付与)
  5. master/job_master/{当日}/job_master.csv と master/job_state/{当日}/job_state.csv へ書込

異常(L2ソース欠落・行数激減)なら何も書込まず exit 1。巡回が完走しなかった媒体が(想定外に)あれば
書込のうえ exit 2。--prune apply を付けたときだけ古い世代を削除する(既定はdry-run表示のみ)。
既定は dry-run。--apply で書込。
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import tempfile
from collections import Counter
from datetime import date

from . import BUCKET, _s3, _today, iter_lines_s3, latest_key
from . import closure, l3, matching, retention, rowcheck

csv.field_size_limit(10**7)

SOURCES = [
    "green",
    "wantedly",
    "bizreach",
    "type",
    "youtrust",
    "ambi",
    "paiza",
    "jobmedley",
    "levtech_career",
    "levtech_freelance",
    "en_tenshoku",
    "townwork",
    "ms_japan",
    "careerconnection",
    "findy",
    "hupro",
    "crowdworks",
    "itpropartners",
    "stanby",
    "mynavi_comedical",
    "mynavi_zeirishi",
    "mynavi_kaikeishi",
]
# mynaviは並列書込の競合回避のためシャード別ソース名(mynavi0..6)で書かれている。
# ここで統合し、job_masterの表示上は source="mynavi" にまとめる。
# 同様の理由で mynavi_shinsotsu/geekly/jac_recruitment/kaigojob/mynavi_kango/
# mynavi_pharma/mynavi_kaigo/mynavi_baito/mynavi_hoiku/levwell_kango/levwell_kaigo
# もシャード分割している。
# 2026-08-27追加: rikunabiは47都道府県・求人詳細ページ計12万件規模を単一プロセスの
# 日次ウィンドウ内で完走できないと判明したため、都道府県を実測求人数で均等分割した
# 12シャード(rikunabi0..rikunabi11)に変更(rikunabi-job-collector側で再設計・
# 詳細はそのREADME参照)。表示名"rikunabi"はCLOSURE_DETECTION_SOURCES側で従来どおり
# 使う(seen_urlsのshard集約ロジックは_load_seen_urls側が対応済み)。
SHARDED_SOURCES = {
    "mynavi": [f"mynavi{i}" for i in range(7)],
    "rikunabi": [f"rikunabi{i}" for i in range(12)],
    "mynavi_shinsotsu": [f"mynavi_shinsotsu{i}" for i in range(4)],
    "geekly": [f"geekly{i}" for i in range(3)],
    "jac_recruitment": [f"jac_recruitment{i}" for i in range(4)],
    "kaigojob": [f"kaigojob{i}" for i in range(6)],
    "mynavi_kango": [f"mynavi_kango{i}" for i in range(6)],
    "mynavi_pharma": [f"mynavi_pharma{i}" for i in range(2)],
    "mynavi_kaigo": [f"mynavi_kaigo{i}" for i in range(4)],
    "mynavi_baito": [f"mynavi_baito{i}" for i in range(10)],
    # 2026-08-27改訂: SHARD_COUNT 6→36(日次ウィンドウ内に完走しないため。
    # mynavi-hoiku-job-collector側で再設計・詳細はそのREADME参照)。
    "mynavi_hoiku": [f"mynavi_hoiku{i}" for i in range(36)],
    "levwell_kango": [f"levwell_kango{i}" for i in range(6)],
    "levwell_kaigo": [f"levwell_kaigo{i}" for i in range(5)],
}
L2_PREFIX = "master/job_sources/"
CM_PREFIX = "master/company_master/"
CM_FILE = "company_master.csv"
JM_PREFIX = "master/job_master/"
JM_FILE = "job_master.csv"
JS_PREFIX = "master/job_state/"
JS_FILE = "job_state.csv"
OUTPUT_SCHEMA = l3.JOB_MASTER_SCHEMA + closure.EXTRA_OUTPUT_COLUMNS


def _load_company_master():
    key = latest_key(CM_PREFIX, CM_FILE)
    if not key:
        raise SystemExit("company_master が見つかりません")
    print(f"  company_master: {key}")
    return csv.DictReader(iter_lines_s3(key))


def _list_dates(prefix: str, fname: str) -> list[str]:
    """`{prefix}{YYYYMMDD}/{fname}` の日付一覧(昇順)。"""
    pat = re.compile(rf"^{re.escape(prefix)}(\d{{8}})/{re.escape(fname)}$")
    out = []
    for page in (
        _s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    ):
        for o in page.get("Contents", []):
            m = pat.match(o["Key"])
            if m:
                out.append(m.group(1))
    return sorted(out)


def _load_prev_state(today: str) -> tuple[dict, str | None]:
    """当日より前の最新 job_state。無ければ空(初回=移行前)。"""
    dates = [d for d in _list_dates(JS_PREFIX, JS_FILE) if d < today]
    if not dates:
        return {}, None
    key = f"{JS_PREFIX}{dates[-1]}/{JS_FILE}"
    state = closure.read_state(iter_lines_s3(key))
    print(f"  前回job_state: {key}  ({len(state):,}件)")
    return state, dates[-1]


def load_seen(source: str, n_shards: int, today: str) -> closure.SourceSeen:
    """当日の `{source}_seen_urls.csv`(シャード媒体は `_shard{N}`)を読む。"""
    prefix = f"raw/job_sources/{source}/{today}/"
    pat = re.compile(
        rf"^{re.escape(prefix)}{re.escape(source)}_seen_urls(?:_shard(\d+))?\.csv$"
    )
    seen = closure.SourceSeen(source, list(range(n_shards)))
    for page in (
        _s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    ):
        for o in page.get("Contents", []):
            m = pat.match(o["Key"])
            if not m:
                continue
            shard = int(m.group(1)) if m.group(1) is not None else 0
            seen.shard_urls.setdefault(shard, set()).update(
                closure.parse_seen_lines(iter_lines_s3(o["Key"]))
            )
    return seen


def _l2_files() -> list[tuple[str, str, str]]:
    """(表示source, L2のsource名, 最新キー)。無いものは除く。"""
    files = []
    pairs = [(s, s) for s in SOURCES]
    pairs += [(disp, sh) for disp, shs in SHARDED_SOURCES.items() for sh in shs]
    for disp, src in pairs:
        key = latest_key(f"{L2_PREFIX}{src}/", f"{src}.csv")
        if key:
            files.append((disp, src, key))
        else:
            print(f"  L2 {src}: なし")
    return files


def _iter_l2(files):
    for fi, (disp, _src, key) in enumerate(files):
        for ri, r in enumerate(csv.DictReader(iter_lines_s3(key))):
            yield fi, ri, disp, r


def build(apply: bool, prune: str = "dry", today_s: str | None = None) -> int:
    today = today_s or _today()
    today_d = date(int(today[:4]), int(today[4:6]), int(today[6:]))

    files = _l2_files()
    for disp, src, key in files:
        print(f"  L2 {src}: {key}")

    cm_index = matching.index_company_master(_load_company_master())
    print(f"company_master索引: {len(cm_index):,}社(正規化名ユニーク)")

    prev, prev_date = _load_prev_state(today)
    if not prev:
        print(
            "  [WARN] 前回job_stateが無い(初回/移行前)。全求人が新規扱い・掲載終了は未確認のまま始まります"
        )

    print("\n=== 掲載終了検知: 当日seen_urls ===")
    seen_by_source = {}
    for src in l3.CLOSURE_DETECTION_SOURCES:
        seen_by_source[src] = load_seen(
            src, len(SHARDED_SOURCES.get(src, [None])), today
        )
    judge = closure.Judge(prev, seen_by_source, l3.CLOSURE_DETECTION_SOURCES, today_d)
    warnings = []
    for src, plan in judge.plans.items():
        sn = seen_by_source[src]
        print(
            f"  {src}: シャード{len(sn.present)}/{len(sn.expected_shards)} / 確認{sn.total():,}件 / "
            f"{plan.mode}{' ' + plan.reason if plan.reason else ''} {plan.detail}"
        )
        warnings += plan.anomalies
        if plan.mode != "judge" and src not in closure.EXPECTED_NO_SEEN:
            warnings.append(f"{src}: 判定保留({plan.reason}) {plan.detail}")

    # 1周目: キーごとに採用する行(scraped_at最新、同値なら先に読んだ方)を決める。
    best: dict[str, tuple] = {}
    vt_acc: Counter = Counter()
    skipped: Counter = Counter()
    for fi, ri, disp, r in _iter_l2(files):
        issue = rowcheck.row_issue(r, today_d)
        if issue:
            skipped[issue] += 1
            continue
        k = closure.norm_key(r.get("job_url", ""))
        judge.vt_past_rate(disp, k, r.get("valid_through", ""), vt_acc)
        cur = best.get(k)
        sa = r.get("scraped_at", "")
        nf = rowcheck.filled_count(r)
        if cur is None or (sa, nf) > (cur[0], cur[3]):
            best[k] = (sa, fi, ri, nf)
    print(f"\n全ソースL2: ユニークURL {len(best):,}件")
    if skipped:
        print(f"  異常行を除外: {dict(skipped)}")
    unreliable = judge.set_vt_reliability(vt_acc)
    if unreliable:
        print(f"  valid_through が過去ばかりで判定に使わない媒体: {sorted(unreliable)}")

    tmp_f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", encoding="utf-8-sig", newline="", delete=False
    )
    tmp_path = tmp_f.name
    w = csv.DictWriter(tmp_f, fieldnames=OUTPUT_SCHEMA, extrasaction="ignore")
    w.writeheader()

    # 2周目: 採用行だけを判定→保持窓→突合→書出。
    new_state: dict[str, closure.St] = {}
    match_stats = Counter()
    method_stats = Counter()
    status_stats: Counter = Counter()
    out_by_source: Counter = Counter()
    n_new = n_dropped = 0
    for fi, ri, disp, r in _iter_l2(files):
        k = closure.norm_key(r.get("job_url", ""))
        b = best.get(k)
        if b is None or b[1] != fi or b[2] != ri:
            continue
        del best[k]
        st = judge.decide_row(k, disp, r.get("valid_through", ""))
        if not closure.within_retention(st, today_d):
            n_dropped += 1
            continue
        new_state[k] = st
        company_name = r.get("company_name", "")
        is_anonymous = matching.is_anonymous_name(company_name)
        if is_anonymous:
            houjin, ambiguous, method, confidence = "", False, "anonymous", ""
        else:
            houjin, ambiguous, method, confidence = matching.match_houjin(
                company_name,
                cm_index,
                location=r.get("location", ""),
                text=" ".join(
                    r.get(c, "")
                    for c in ("description_snippet", "appeal_points", "job_title")
                ),
                job_url=r.get("job_url", ""),
            )
        match_stats[
            "matched" if houjin else ("ambiguous" if ambiguous else "unmatched")
        ] += 1
        method_stats[method] += 1
        is_new = k not in prev
        n_new += is_new
        status_stats[(disp, st.status, st.reason)] += 1
        out_by_source[disp] += 1

        row = {c: r.get(c, "") for c in l3.JOB_MASTER_SCHEMA}
        row["source"] = disp
        row["valid_through"] = rowcheck.normalize_valid_through(
            r.get("valid_through", ""), r.get("scraped_at", "")
        )
        row["houjin_bangou"] = houjin
        row["is_ambiguous_company"] = "1" if ambiguous else ""
        row["match_method"] = method
        row["match_confidence"] = confidence
        row["is_staffing_agent"] = (
            "1" if matching.is_staffing_agent_name(company_name) else ""
        )
        row["is_anonymous"] = "1" if is_anonymous else ""
        row["first_seen_date"] = st.first_seen
        row["is_new_today"] = "1" if is_new else "0"
        row["listing_status"] = st.status
        row["closed_detected_date"] = st.closed_date
        row["last_seen_date"] = st.last_seen
        row["status_reason"] = st.reason
        w.writerow(row)
    tmp_f.close()

    print("\n=== company_master突合 ===")
    print(
        f"  マッチ: {match_stats['matched']:,} / 同名複数: {match_stats['ambiguous']:,} / 未マッチ: {match_stats['unmatched']:,}"
    )
    print(f"  内訳: {dict(method_stats)}")
    print(
        f"\n=== 新規掲載検知 ===\n  本日新規: {n_new:,} / 既存: {len(new_state) - n_new:,}"
    )
    print(
        f"\n=== 状態(source別 listing_status/status_reason) 保持窓で除外 {n_dropped:,}件 ==="
    )
    for (src, status, reason), n in sorted(status_stats.items()):
        print(f"  {src}: {status}/{reason} = {n:,}")

    errors = []
    if not prev and apply:
        errors.append(
            "前回job_stateが無いため書込しません(先に job_sl.migrate_state を実行)"
        )
    if prev:
        if len(new_state) < len(prev) * (1 - closure.MAX_ROW_DROP_RATIO):
            errors.append(
                f"状態の行数が前回比{len(new_state) / len(prev):.0%}に激減({len(prev):,}→{len(new_state):,})"
            )
        prev_by_src = Counter(s.source for s in prev.values())
        for src, n in prev_by_src.items():
            if n >= 1000 and out_by_source.get(src, 0) < n * 0.5:
                errors.append(
                    f"{src}: 出力{out_by_source.get(src, 0):,}件 / 前回{n:,}件。L2が欠落/縮小している疑い"
                )
    for e in errors:
        print(f"[ERROR] {e}")
    for wmsg in warnings:
        print(f"[WARN] {wmsg}")
    if errors:
        os.remove(tmp_path)
        print("異常のため書込しません")
        return 1

    if not apply:
        os.replace(tmp_path, "job_master_dryrun.csv")
        print("\n[dry-run] ローカル job_master_dryrun.csv に出力。S3へは書込まない。")
    else:
        # 5GB超のためマルチパート(upload_file)。ディスク上の一時ファイルから直接アップロードする。
        state_path = tmp_path + ".state"
        with open(state_path, "w", encoding="utf-8-sig", newline="") as sf:
            closure.write_state(sf, new_state)
        try:
            _s3().upload_file(state_path, BUCKET, f"{JS_PREFIX}{today}/{JS_FILE}")
            _s3().upload_file(tmp_path, BUCKET, f"{JM_PREFIX}{today}/{JM_FILE}")
            print(
                f"\n[apply] 書込完了: s3://{BUCKET}/{JM_PREFIX}{today}/{JM_FILE} (+ {JS_PREFIX}{today}/{JS_FILE})"
            )
        finally:
            os.remove(tmp_path)
            os.remove(state_path)
        _prune(prune == "apply", [src for _d, src, _k in files])
    return 2 if warnings else 0


def _prune(do_delete: bool, l2_sources: list[str]) -> None:
    dry = not do_delete
    retention.prune_generations(
        JM_PREFIX, JM_FILE, retention.KEEP_JOB_MASTER, dry_run=dry
    )
    retention.prune_generations(
        JS_PREFIX, JS_FILE, retention.KEEP_JOB_STATE, dry_run=dry
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="S3へ書込(既定はdry-run)")
    ap.add_argument(
        "--prune",
        choices=["dry", "apply"],
        default="dry",
        help="書込後の世代削除。apply のときだけ削除(既定は削除予定の表示のみ)",
    )
    a = ap.parse_args()
    sys.exit(build(a.apply, a.prune))


if __name__ == "__main__":
    main()
