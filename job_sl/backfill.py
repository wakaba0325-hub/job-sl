"""求人データの過去分クリーンアップ(一回限り)。

    python -m job_sl.backfill audit                         # 異常の全量スキャン(読取のみ)
    python -m job_sl.backfill rebuild-l2 [--apply]          # 凍結したL2をL1から作り直す
    python -m job_sl.backfill replay-state --end YYYYMMDD [--apply]   # 過去のseenから job_state を復元
    python -m job_sl.backfill prune [--apply]               # 古い世代を整理(上限付き)

全コマンドが既定 dry-run(S3へは書かない・消さない)。L1(raw/job_sources の原本)には一切書かない・消さない。

replay-state は migrate_state の代わり(同じ master/job_state/{end}/ に書く)。過去の seen_urls を日付順に
本番と同じ closure.Judge に通し、求人URLごとの first_seen / last_seen / 状態 / 終了検知日を復元する。
  - first_seen_date: L2 の scraped_at の日付(L1は新規URLのみ追記なので初回取得日。シャード間の重複行は最小)
  - seen のある日: closure.py の遷移そのまま(連続2回未確認でclosed、シャード欠け・安全装置での保留)
  - closed_detected_date: 最後に確認できた日の翌日(確認履歴が無ければ valid_through、無ければ最初に判定できた日)
  - 一度も確認できないまま閉じた行は status_reason=backfill_never_seen(推定。seen の期間より前に消えた可能性)
  - seen の無い媒体・期間は valid_through だけで閉じ、他は unknown のまま(根拠なく閉じない)
  - 終了から365日を超えた行(valid_through が昔の求人など)は状態から外す
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from . import BUCKET, _s3, _today, iter_lines_s3
from . import build_l3, closure, l3, rowcheck
from .l2_stream import build_l2_stream, merge_l1

csv.field_size_limit(10**7)

RAW_PREFIX = "raw/job_sources/"
L2_PREFIX = build_l3.L2_PREFIX
JM_PREFIX, JM_FILE = build_l3.JM_PREFIX, build_l3.JM_FILE
JS_PREFIX, JS_FILE = build_l3.JS_PREFIX, build_l3.JS_FILE

KEEP_JOB_MASTER = 7
KEEP_JOB_STATE = 14
KEEP_L2 = 14
DEFAULT_MAX_PRUNE_OBJECTS = (
    200  # 1回の prune --apply で消す(退避する)オブジェクト数の上限
)

_RAW_L1 = re.compile(r"^raw/job_sources/([^/]+)/(\d{8})/\1\.csv$")
_RAW_SEEN = re.compile(
    r"^raw/job_sources/([^/]+)/(\d{8})/\1_seen_urls(?:_shard\d+)?\.csv$"
)
_L2 = re.compile(r"^master/job_sources/([^/]+)/(\d{8})/\1\.csv$")


def _iso(d8: str) -> str:
    return f"{d8[:4]}-{d8[4:6]}-{d8[6:]}"


def _d8_to_date(d8: str) -> date:
    return date(int(d8[:4]), int(d8[4:6]), int(d8[6:]))


def list_objects(prefix: str) -> List[Tuple[str, int]]:
    out = []
    for page in (
        _s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    ):
        for o in page.get("Contents", []):
            out.append((o["Key"], o.get("Size", 0)))
    return out


def key_exists(key: str) -> bool:
    return any(k == key for k, _ in list_objects(key))


# ---- audit ---------------------------------------------------------------------


def date_folder_anomalies(objs: Iterable[Tuple[str, int]], today: date) -> List[str]:
    """日付フォルダが未来日付・8桁でない・存在しない日付のキー。latest_key が最新と取り違えるので要注意。"""
    out = []
    for key, _ in objs:
        for part in key.split("/")[2:4]:
            if re.fullmatch(r"\d{8}", part):
                try:
                    if _d8_to_date(part) > today:
                        out.append(f"future_folder:{key}")
                except ValueError:
                    out.append(f"invalid_date_folder:{key}")
                break
            if re.match(r"\d{4}-\d{2}-\d{2}$", part):
                out.append(f"hyphen_date_folder:{key}")
                break
    return out


def audit(today_s: Optional[str] = None, out_path: str = "backfill_audit.json") -> dict:
    """最新L2を全ソース走査して異常行を数える。読取のみ。"""
    today_s = today_s or _today()
    today_d = _d8_to_date(today_s)
    files = build_l3._l2_files()
    raw_objs = list_objects(RAW_PREFIX)
    l2_objs = list_objects(L2_PREFIX)
    jm_objs = list_objects(JM_PREFIX)
    folder_issues = (
        date_folder_anomalies(raw_objs, today_d)
        + date_folder_anomalies(l2_objs, today_d)
        + date_folder_anomalies(jm_objs, today_d)
    )
    per_source: Dict[str, Counter] = defaultdict(Counter)
    key_source: Dict[str, str] = {}
    cross_dup = Counter()
    for disp, src, key in files:
        c = per_source[disp]
        for r in csv.DictReader(iter_lines_s3(key)):
            c["rows"] += 1
            issue = rowcheck.row_issue(r, today_d)
            if issue:
                c[issue] += 1
                continue
            k = closure.norm_key(r["job_url"])
            if k in key_source:
                cross_dup[disp] += 1
                c["dup_rows"] += 1
            else:
                key_source[k] = disp
            pd = (r.get("posted_date") or "")[:10]
            if re.match(r"\d{4}-\d{2}-\d{2}$", pd) and pd > today_d.isoformat():
                c["future_posted_date"] += 1
            if not r.get("scraped_at"):
                c["no_scraped_at"] += 1
    stale = stale_l2(raw_objs, l2_objs)
    report = {
        "today": today_s,
        "rows_by_source": {s: dict(c) for s, c in sorted(per_source.items())},
        "unique_urls": len(key_source),
        "total_rows": sum(c["rows"] for c in per_source.values()),
        "folder_anomalies": folder_issues,
        "stale_l2": [dict(zip(("source", "l1_latest", "l2_latest"), t)) for t in stale],
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    print(
        f"[audit] 行{report['total_rows']:,} / ユニークURL {report['unique_urls']:,} / "
        f"フォルダ異常 {len(folder_issues)} / L2凍結 {len(stale)}ソース -> {out_path}"
    )
    issues = Counter()
    for c in per_source.values():
        for k, v in c.items():
            if k not in ("rows",):
                issues[k] += v
    print(f"  行の異常: {dict(issues)}")
    return report


# ---- rebuild-l2 ----------------------------------------------------------------


def stale_l2(
    raw_objs: Iterable[Tuple[str, int]], l2_objs: Iterable[Tuple[str, int]]
) -> List[Tuple[str, str, str]]:
    """L1の最新日がL2の最新日より新しいソース(build_l2が失敗/OOMで凍結)。(source, L1最新, L2最新)。"""
    l1: Dict[str, str] = {}
    l2: Dict[str, str] = {}
    for key, _ in raw_objs:
        m = _RAW_L1.match(key)
        if m:
            l1[m.group(1)] = max(l1.get(m.group(1), ""), m.group(2))
    for key, _ in l2_objs:
        m = _L2.match(key)
        if m:
            l2[m.group(1)] = max(l2.get(m.group(1), ""), m.group(2))
    return sorted((s, l1[s], l2.get(s, "")) for s in l1 if l1[s] > l2.get(s, ""))


def rebuild_l2(
    sources: List[str],
    apply: bool,
    scan: bool = False,
    today_s: Optional[str] = None,
    max_sources: int = 3,
) -> List[str]:
    """凍結したL2をL1全履歴から作り直し、当日フォルダへ新規書込(既存キーは上書きしない)。
    1回の実行は max_sources まで。メモリは1ソースのL1ぶん(mynavi_baito は約2GB/シャード→16GB以上のタスクで)。"""
    today_s = today_s or _today()
    stale = {
        s: (a, b)
        for s, a, b in stale_l2(list_objects(RAW_PREFIX), list_objects(L2_PREFIX))
    }
    if not sources:
        for s, (a, b) in stale.items():
            print(f"  凍結: {s}  L1最新 {a} / L2最新 {b or '-'}")
        print(f"[rebuild-l2] 凍結 {len(stale)}ソース。--sources で対象を指定")
        return []
    todo = []
    for s in sources:
        if s not in stale:
            print(f"  {s}: 凍結していない(L1最新 <= L2最新)のでスキップ")
            continue
        todo.append(s)
    if len(todo) > max_sources:
        print(f"  [上限] 1回は {max_sources} ソースまで。残り {todo[max_sources:]}")
        todo = todo[:max_sources]
    done = []
    for s in todo:
        key = f"{L2_PREFIX}{s}/{today_s}/{s}.csv"
        if key_exists(key):
            print(f"  {s}: {key} が既にあるため書込まない")
            continue
        a, b = stale[s]
        if not apply:
            msg = f"  [dry-run] {s}: L1 {a} / L2 {b or '-'} -> {key}"
            if scan:
                msg += f"  (L1から {len(merge_l1(s)):,} 件になる見込み)"
            print(msg)
            continue
        _rows, k = build_l2_stream(s, run_date=today_s, write=True)
        print(f"  [apply] {s}: s3://{BUCKET}/{k}")
        done.append(s)
    return done


# ---- replay-state ----------------------------------------------------------------

# universe の値: (表示source, valid_through, first(初回取得日 YYYY-MM-DD), scraped_at)
Entry = Tuple[str, str, str, str]


def load_universe(
    files: List[Tuple[str, str, str]], today_d: date
) -> Tuple[Dict[str, Entry], Counter]:
    """最新L2の全ソースをURL(小文字・trim)キーで畳む。残す行は scraped_at が新しい方(同値は情報が多い方)、
    first は重複行の最小 scraped_at の日付(シャード間でURLが入れ替わる媒体の初回取得日を失わない)。"""
    best: Dict[str, Entry] = {}
    nfill: Dict[str, int] = {}
    stats: Counter = Counter()
    for disp, _src, key in files:
        for r in csv.DictReader(iter_lines_s3(key)):
            stats["rows"] += 1
            issue = rowcheck.row_issue(r, today_d)
            if issue:
                stats[issue] += 1
                continue
            k = closure.norm_key(r["job_url"])
            sa = r.get("scraped_at", "")
            nf = rowcheck.filled_count(r)
            cur = best.get(k)
            if cur is None:
                best[k] = (disp, r.get("valid_through", ""), sa[:10], sa)
                nfill[k] = nf
                continue
            stats["dup_rows"] += 1
            firsts = [x for x in (cur[2], sa[:10]) if x]
            first = min(firsts) if firsts else ""
            if (sa, nf) > (cur[3], nfill[k]):
                best[k] = (disp, r.get("valid_through", ""), first, sa)
                nfill[k] = nf
            else:
                best[k] = (cur[0], cur[1], first, cur[3])
    stats["unique"] = len(best)
    return best, stats


def seen_days(raw_objs: Iterable[Tuple[str, int]], sources: Iterable[str]) -> List[str]:
    """seen_urls が存在する日付(いずれかの対象媒体)。"""
    want = set(sources)
    days = set()
    for key, _ in raw_objs:
        m = _RAW_SEEN.match(key)
        if m and m.group(1) in want:
            days.add(m.group(2))
    return sorted(days)


def replay(
    uni: Dict[str, Entry],
    days: List[str],
    end_d8: str,
    load_seen=None,
    log=print,
) -> Tuple[Dict[str, closure.St], dict]:
    """days(YYYYMMDD昇順)を順に closure.Judge へ通して状態を復元する。戻り値は (end時点の状態, 集計)。"""
    load_seen = load_seen or build_l3.load_seen
    end_d = _d8_to_date(end_d8)
    sources = l3.CLOSURE_DETECTION_SOURCES
    by_first: Dict[str, List[str]] = defaultdict(list)
    for k, e in uni.items():
        if e[2] and e[2] > end_d.isoformat():
            continue
        by_first[e[2] or ""].append(k)
    first_dates = sorted(by_first)
    ptr = 0
    entered: List[str] = []
    prev: Dict[str, closure.St] = {}
    daily = []
    for d8 in days:
        d = _d8_to_date(d8)
        d_iso = d.isoformat()
        while ptr < len(first_dates) and first_dates[ptr] <= d_iso:
            entered.extend(by_first[first_dates[ptr]])
            ptr += 1
        seen_by_source = {
            s: load_seen(s, len(build_l3.SHARDED_SOURCES.get(s, [None])), d8)
            for s in sources
        }
        judge = closure.Judge(prev, seen_by_source, sources, d)
        acc: Counter = Counter()
        for s, ss in seen_by_source.items():
            for k in ss.url_shard():
                e = uni.get(k)
                if e is not None and e[0] == s and (not e[2] or e[2] <= d_iso):
                    judge.vt_past_rate(s, k, e[1], acc)
        judge.set_vt_reliability(acc)
        new: Dict[str, closure.St] = {}
        for k in entered:
            e = uni[k]
            p = prev.get(k)
            st = judge.decide_row(k, e[0], e[1])
            if p is None:
                st = st._replace(first_seen=min(e[2] or d_iso, d_iso))
            if st.status == "closed" and (p is None or p.status != "closed"):
                st = _label_closed(st)
            new[k] = st
        prev = new
        cnt = Counter(s.status for s in new.values())
        holds = {s: p.reason for s, p in judge.plans.items() if p.mode != "judge"}
        daily.append({"day": d8, "states": dict(cnt), "holds": holds})
        log(f"  {d8}: {len(new):,}件 {dict(cnt)} 保留媒体{len(holds)}")
    final = {k: s for k, s in prev.items() if closure.within_retention(s, end_d)}
    summary = {
        "days": len(days),
        "universe": len(uni),
        "entered": len(entered),
        "before_retention": len(prev),
        "dropped_by_retention": len(prev) - len(final),
        "dropped_by_source": dict(
            Counter(s.source for k, s in prev.items() if k not in final)
        ),
        "daily": daily,
    }
    return final, summary


def _label_closed(st: closure.St) -> closure.St:
    """新たに閉じた行の検知日と理由を過去分向けに付け直す。"""
    if st.reason != "not_seen":
        return st
    ls = closure.parse_date(st.last_seen)
    if ls is None:
        return st._replace(reason="backfill_never_seen")
    return st._replace(closed_date=closure.iso(ls + timedelta(days=1)))


def replay_state(
    end_d8: str,
    apply: bool,
    out_dir: str = "backfill_out",
    start_d8: Optional[str] = None,
    today_s: Optional[str] = None,
) -> int:
    today_s = today_s or _today()
    today_d = _d8_to_date(today_s)
    if _d8_to_date(end_d8) > today_d:
        print(f"[ERROR] --end {end_d8} が未来日です")
        return 1
    key = f"{JS_PREFIX}{end_d8}/{JS_FILE}"
    if apply and key_exists(key):
        print(f"[ERROR] {key} が既にあります(上書きしません)")
        return 1
    files = build_l3._l2_files()
    raw_objs = list_objects(RAW_PREFIX)
    days = [
        d
        for d in seen_days(raw_objs, l3.CLOSURE_DETECTION_SOURCES)
        if d <= end_d8 and (not start_d8 or d >= start_d8)
    ]
    if not days:
        print("[ERROR] 対象日のseenがありません")
        return 1
    print(f"seen日 {days[0]}..{days[-1]} ({len(days)}日) / L2 {len(files)}ファイル")
    if days[-1] != end_d8:
        print(
            f"  [WARN] --end {end_d8} のseenはまだ無い。最後のseen日 {days[-1]} までの状態を {end_d8} として書く"
        )
    uni, ustats = load_universe(files, today_d)
    print(
        f"universe: ユニークURL {len(uni):,} / 行 {ustats['rows']:,} / 異常・重複 { {k: v for k, v in ustats.items() if k not in ('rows', 'unique')} }"
    )
    final, summary = replay(uni, days, end_d8)
    summary["universe_stats"] = dict(ustats)
    by = Counter((s.status, s.reason) for s in final.values())
    print(
        f"\n復元した状態 {len(final):,}件 (保持窓で外れた {summary['dropped_by_retention']:,}件)"
    )
    for (status, reason), n in sorted(by.items(), key=lambda x: -x[1]):
        print(f"  {status}/{reason}: {n:,}")
    os.makedirs(out_dir, exist_ok=True)
    state_path = os.path.join(out_dir, "job_state_replayed.csv")
    with open(state_path, "w", encoding="utf-8-sig", newline="") as f:
        closure.write_state(f, final)
    with open(os.path.join(out_dir, "replay_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    if not apply:
        print(f"[dry-run] {state_path} に出力。S3へは書込まない")
        return 0
    _s3().upload_file(state_path, BUCKET, key)
    print(f"[apply] s3://{BUCKET}/{key}")
    return 0


# ---- prune ---------------------------------------------------------------------


def _groups(
    objs: Iterable[Tuple[str, int]], keep_jm: int, keep_js: int, keep_l2: int
) -> List[Tuple[str, str, str, int, Dict[str, Tuple[str, int]]]]:
    """(kind, prefix, fname, keep, {date8: (key, size)}) を返す。"""
    jm: Dict[str, Tuple[str, int]] = {}
    js: Dict[str, Tuple[str, int]] = {}
    l2: Dict[str, Dict[str, Tuple[str, int]]] = defaultdict(dict)
    for key, size in objs:
        m = re.match(rf"^{re.escape(JM_PREFIX)}(\d{{8}})/{re.escape(JM_FILE)}$", key)
        if m:
            jm[m.group(1)] = (key, size)
            continue
        m = re.match(rf"^{re.escape(JS_PREFIX)}(\d{{8}})/{re.escape(JS_FILE)}$", key)
        if m:
            js[m.group(1)] = (key, size)
            continue
        m = _L2.match(key)
        if m:
            l2[m.group(1)][m.group(2)] = (key, size)
    out = [
        ("job_master", JM_PREFIX, JM_FILE, keep_jm, jm),
        ("job_state", JS_PREFIX, JS_FILE, keep_js, js),
    ]
    for src in sorted(l2):
        out.append(("l2", f"{L2_PREFIX}{src}/", f"{src}.csv", keep_l2, l2[src]))
    return out


def plan_prune(
    objs: Iterable[Tuple[str, int]],
    today_s: str,
    keep_jm: int = KEEP_JOB_MASTER,
    keep_js: int = KEEP_JOB_STATE,
    keep_l2: int = KEEP_L2,
    protect: Iterable[str] = (),
) -> Tuple[List[tuple], List[str]]:
    """削除(退避)候補を古い順に返す。戻り値は ([(kind, key, size, date8, backup_key)], 注意メッセージ)。
    件数基準(新しい順にN世代残す)。未来日付の世代は数えず触らない。最新世代が空なら、そのグループは何もしない。"""
    protect = set(protect)
    victims: List[tuple] = []
    notes: List[str] = []
    for kind, prefix, fname, keep, gens in _groups(objs, keep_jm, keep_js, keep_l2):
        future = sorted(d for d in gens if d > today_s)
        for d in future:
            notes.append(f"未来日付の世代は触らない(要確認): {gens[d][0]}")
        dates = sorted((d for d in gens if d <= today_s), reverse=True)
        if len(dates) <= keep:
            continue
        if not gens[dates[0]][1]:
            notes.append(f"{prefix}: 最新 {dates[0]} が空のため何も消さない")
            continue
        for d in sorted(dates[keep:]):
            if d in protect:
                continue
            key, size = gens[d]
            victims.append((kind, key, size, d, f"{prefix}_backup/{d}/{fname}"))
    order = {"job_master": 0, "job_state": 1, "l2": 2}
    victims.sort(key=lambda v: (order[v[0]], v[3], v[1]))
    return victims, notes


def versioning_enabled() -> bool:
    try:
        return _s3().get_bucket_versioning(Bucket=BUCKET).get("Status") == "Enabled"
    except Exception:
        return False


def prune(
    apply: bool,
    mode: str = "delete",
    max_objects: int = DEFAULT_MAX_PRUNE_OBJECTS,
    today_s: Optional[str] = None,
    protect: Iterable[str] = (),
    keep_jm: int = KEEP_JOB_MASTER,
    keep_js: int = KEEP_JOB_STATE,
    keep_l2: int = KEEP_L2,
) -> int:
    """古い世代の削除(delete marker)または _backup/ への退避。dry-run既定、1回 max_objects まで。
    L1(raw/job_sources)・seen・_raw/_backup/_state は対象外(キー形式が合うものだけ)。"""
    today_s = today_s or _today()
    objs = [o for p in (JM_PREFIX, JS_PREFIX, L2_PREFIX) for o in list_objects(p)]
    victims, notes = plan_prune(objs, today_s, keep_jm, keep_js, keep_l2, protect)
    for n in notes:
        print(f"  [注意] {n}")
    total = sum(v[2] for v in victims)
    kinds = Counter(v[0] for v in victims)
    print(
        f"[prune] 対象 {len(victims)}オブジェクト {total / 1e9:,.1f}GB {dict(kinds)} "
        f"(残す: job_master {keep_jm} / job_state {keep_js} / L2 {keep_l2} 世代)"
    )
    batch = victims[:max_objects]
    if len(victims) > len(batch):
        print(
            f"  [上限] 今回は先頭 {len(batch)} 件(--max-objects {max_objects})。残り {len(victims) - len(batch)} 件は次回"
        )
    for kind, key, size, d, bkey in batch[:5]:
        print(f"  {'dry-run ' if not apply else ''}{mode}: {key} ({size / 1e6:,.0f}MB)")
    if not apply:
        print("[dry-run] 何も変更していません")
        return 0
    if not versioning_enabled():
        print("[ERROR] バケットのversioningがEnabledではないため中止")
        return 1
    s3 = _s3()
    deleted = 0
    for i in range(0, len(batch), 1000):
        chunk = batch[i : i + 1000]
        if mode == "backup":
            for kind, key, size, d, bkey in chunk:
                s3.copy({"Bucket": BUCKET, "Key": key}, BUCKET, bkey)
        s3.delete_objects(
            Bucket=BUCKET,
            Delete={"Objects": [{"Key": v[1]} for v in chunk], "Quiet": True},
        )
        deleted += len(chunk)
    print(
        f"[apply] {mode}: {deleted}オブジェクト (delete marker。復元はノンカレント版の削除猶予内)"
    )
    return 0


# ---- CLI -----------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("audit", help="異常の全量スキャン(読取のみ)")
    a.add_argument("--out", default="backfill_audit.json")

    r = sub.add_parser("rebuild-l2", help="凍結したL2をL1から作り直す")
    r.add_argument(
        "--sources", default="", help="カンマ区切り。省略時は凍結ソースの一覧のみ"
    )
    r.add_argument(
        "--scan", action="store_true", help="dry-runでもL1を読んで件数を出す"
    )
    r.add_argument("--max-sources", type=int, default=3)
    r.add_argument("--apply", action="store_true")

    p = sub.add_parser("replay-state", help="過去のseenから job_state を復元")
    p.add_argument(
        "--end", required=True, help="状態の日付 YYYYMMDD(= master/job_state/{end}/)"
    )
    p.add_argument("--start", help="最初のseen日(省略時は最古)")
    p.add_argument("--out-dir", default="backfill_out")
    p.add_argument("--apply", action="store_true")

    q = sub.add_parser("prune", help="古い世代の整理(上限付き)")
    q.add_argument("--mode", choices=["delete", "backup"], default="delete")
    q.add_argument("--max-objects", type=int, default=DEFAULT_MAX_PRUNE_OBJECTS)
    q.add_argument("--protect", default="", help="消さない日付(YYYYMMDD,カンマ区切り)")
    q.add_argument("--keep-job-master", type=int, default=KEEP_JOB_MASTER)
    q.add_argument("--keep-job-state", type=int, default=KEEP_JOB_STATE)
    q.add_argument("--keep-l2", type=int, default=KEEP_L2)
    q.add_argument("--apply", action="store_true")

    a_ = ap.parse_args(argv)
    if a_.cmd == "audit":
        audit(out_path=a_.out)
        return 0
    if a_.cmd == "rebuild-l2":
        srcs = [s for s in a_.sources.split(",") if s]
        rebuild_l2(srcs, a_.apply, a_.scan, max_sources=a_.max_sources)
        return 0
    if a_.cmd == "replay-state":
        if not re.fullmatch(r"\d{8}", a_.end):
            sys.exit("--end は YYYYMMDD")
        return replay_state(a_.end, a_.apply, a_.out_dir, a_.start)
    return prune(
        a_.apply,
        a_.mode,
        a_.max_objects,
        protect=[x for x in a_.protect.split(",") if x],
        keep_jm=a_.keep_job_master,
        keep_js=a_.keep_job_state,
        keep_l2=a_.keep_l2,
    )


if __name__ == "__main__":
    sys.exit(main())
