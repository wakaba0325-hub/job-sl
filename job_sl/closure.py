"""掲載終了検知と求人状態(job_state)の遷移ロジック。I/Oを持たない純粋関数中心。

状態テーブル `master/job_state/{YYYYMMDD}/job_state.csv` は求人URL(小文字化・trim)単位の1行で、
前日までの状態 + 当日のL2行集合 + 当日のseen_urls から当日の状態を作る。

listing_status:
  active  当日の巡回で存在を確認できた(last_seen_date=当日)
  closed  巡回が完走した日に確認できなかった / valid_through が過去(closed_detected_date以降1年保持)
  unknown 判定不能。理由は status_reason に残す

status_reason:
  seen                    当日確認できた
  not_seen                完走した巡回で確認できなかった(=掲載終了)
  valid_through_expired   確認できない状態で valid_through が過去日(補助判定)
  no_seen_source          seen_urlsを出さない媒体(判定対象外)
  no_seen_file            当日のseen_urlsが無い(巡回が完走しなかった)
  shard_incomplete        一部シャードのseenが無い/閾値未満。そのシャード分だけ保留
  source_partial_unstable シャード欠け。URLのシャード割当が日次で変わる媒体なので媒体全体を保留
  safety_ratio            確認件数が前日activeの閾値未満(巡回不完全の疑い)で媒体全体を保留
  miss_pending            完走した巡回で1回確認できなかった(連続 CLOSE_AFTER_MISSES 回で closed。1日欠けて戻る求人の誤判定防止)
  stale_unconfirmed       activeだったが STALE_ACTIVE_DAYS 日以上確認できず保留が続いた
  migrated                job_master からの移行直後で未判定

シャード欠けの扱い: seen_urlsはサイト全体のURL一覧を等分割したもので、シャードは互いに素。
前日にそのURLを確認したシャード(last_seen_shard)が当日のURLでも同じ媒体(割当が安定、同日確認分の
一致率が STABLE_SHARD_RATE 以上)なら、欠けたシャード分だけ保留し他は判定する。安定でない媒体は
一部でも欠ければ媒体全体を保留する(欠けたシャードに入るURLが特定できないため)。
"""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, Iterable, Iterator, NamedTuple, Optional

CLOSED_RETENTION_DAYS = 365  # 掲載終了の検知日からこの日数は「掲載終了」として残す
UNKNOWN_RETENTION_DAYS = 365  # 判定不能のまま確認も取得もない行を残す日数
CLOSE_AFTER_MISSES = 2  # 完走した巡回でこの回数続けて確認できなかったら closed
STALE_ACTIVE_DAYS = 3  # 保留が続いてもactiveを維持する日数。超えたらunknown
SAFETY_THRESHOLD_RATIO = (
    0.7  # 当日確認件数 / 前日active件数 がこれ未満なら巡回不完全とみなす
)
STABLE_SHARD_RATE = (
    0.98  # 前日と同じシャードに入ったURLの割合。以上なら割当が安定とみなす
)
VT_UNRELIABLE_RATIO = 0.05  # 当日確認できた行のうち valid_through が過去の割合。超える媒体は valid_through を判定に使わない
MAX_ROW_DROP_RATIO = 0.2  # 状態の行数が前回比でこれ以上減ったら異常(書込まない)
SUSPECT_CAP_MIN = (
    100_000  # 確認件数がこの値以上かつ10000の倍数なら上限打ち切りの疑いとして警告
)

STATE_COLUMNS = [
    "job_key",
    "source",
    "first_seen_date",
    "last_seen_date",
    "last_seen_shard",
    "listing_status",
    "status_reason",
    "closed_detected_date",
    "miss_count",
]

# L2/L3 への追加出力列(job_master の末尾に付く)
EXTRA_OUTPUT_COLUMNS = ["last_seen_date", "status_reason"]

# seen_urls を出さない/出しても判定に使わない媒体(掲載終了検知の対象外・警告も出さない)
# 取得網羅性がない(stanby/mynavi_baito)・安定しない(ms_japan)・停止中(bizreach)など
# kaigojob は collector が完走せず seen を書けていない(再開したら外す)
EXPECTED_NO_SEEN = {"bizreach", "ms_japan", "stanby", "mynavi_baito", "kaigojob"}


class St(NamedTuple):
    source: str
    first_seen: str
    last_seen: str
    last_shard: str  # "" = 不明/非シャード
    status: str
    reason: str
    closed_date: str
    misses: str = "0"  # 完走した巡回で連続して確認できなかった回数


def norm_key(url: str) -> str:
    """状態のキー。L2のdedupキー(job_sl.key_for)と同じ規則。"""
    return (url or "").strip().lower()


def iso(d: date) -> str:
    return d.isoformat()


def parse_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    if len(s) < 10:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


# ---- seen_urls ---------------------------------------------------------------


@dataclass
class SourceSeen:
    """1媒体の当日seen。shard_urls[shard] = そのシャードのURL集合(非シャード媒体は shard=0 のみ)。"""

    source: str
    expected_shards: list  # 期待するシャード番号 [0..n-1]。非シャードは [0]
    shard_urls: Dict[int, set] = field(default_factory=dict)

    @property
    def present(self) -> set:
        return set(self.shard_urls) & set(self.expected_shards)

    @property
    def missing(self) -> set:
        return set(self.expected_shards) - set(self.shard_urls)

    def url_shard(self) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for sh in sorted(self.present):
            for u in self.shard_urls[sh]:
                out.setdefault(u, sh)
        return out

    def total(self) -> int:
        return sum(len(self.shard_urls[s]) for s in self.present)


def parse_seen_lines(lines: Iterable[str]) -> Iterator[str]:
    """seen_urls CSV(ヘッダ job_url)の行から正規化キーを返す。"""
    r = csv.reader(lines)
    first = True
    for row in r:
        if not row:
            continue
        if first:
            first = False
            if row[0].strip().lstrip("﻿") == "job_url":
                continue
        k = norm_key(row[0].lstrip("﻿"))
        if k:
            yield k


# ---- 媒体ごとの判定計画 --------------------------------------------------------


@dataclass
class SourcePlan:
    source: str
    mode: str  # "judge"(全件判定) / "partial"(judged_shardsのみ判定) / "hold"(全件保留)
    reason: str = ""  # hold/partial の理由
    judged_shards: set = field(default_factory=set)
    detail: str = ""
    anomalies: list = field(default_factory=list)


def shard_stability(prev: Dict[str, St], seen: SourceSeen) -> Optional[float]:
    """当日確認できたURLのうち、前日確認時と同じシャードに入った割合。比較対象が無ければ None。"""
    same = total = 0
    for sh in seen.present:
        for u in seen.shard_urls[sh]:
            p = prev.get(u)
            if p is None or p.last_shard == "":
                continue
            total += 1
            if p.last_shard == str(sh):
                same += 1
    return same / total if total else None


def plan_source(
    source: str,
    seen: Optional[SourceSeen],
    prev_active_total: int,
    prev_active_by_shard: Counter,
    stability: Optional[float],
    ratio: float = SAFETY_THRESHOLD_RATIO,
) -> SourcePlan:
    if seen is None or not seen.present:
        return SourcePlan(source, "hold", "no_seen_file", detail="seenファイル無し")
    anomalies = []
    total = seen.total()
    if total >= SUSPECT_CAP_MIN and total % 10000 == 0:
        anomalies.append(f"{source}: 確認件数{total:,}が丸い数字(上限打ち切りの疑い)")

    if not seen.missing:
        if prev_active_total > 0 and total / prev_active_total < ratio:
            return SourcePlan(
                source,
                "hold",
                "safety_ratio",
                detail=f"確認{total:,}/前日active{prev_active_total:,}={total / prev_active_total:.1%} < {ratio:.0%}",
                anomalies=anomalies,
            )
        if total == 0:
            return SourcePlan(source, "hold", "no_seen_file", detail="seenが0件")
        return SourcePlan(source, "judge", anomalies=anomalies)

    miss = sorted(seen.missing)
    if stability is None or stability < STABLE_SHARD_RATE:
        st = "比較対象なし" if stability is None else f"{stability:.0%}"
        return SourcePlan(
            source,
            "hold",
            "source_partial_unstable",
            detail=f"シャード{miss}が欠け・割当安定度{st}のため媒体全体を保留",
            anomalies=anomalies,
        )
    judged = set()
    for sh in sorted(seen.present):
        prev_n = prev_active_by_shard.get(str(sh), 0)
        n = len(seen.shard_urls[sh])
        if prev_n > 0 and n / prev_n < ratio:
            continue
        judged.add(sh)
    held = sorted(set(seen.expected_shards) - judged)
    return SourcePlan(
        source,
        "partial",
        "shard_incomplete",
        judged_shards=judged,
        detail=f"シャード{held}を保留(欠け{miss})・{len(judged)}/{len(seen.expected_shards)}シャードを判定",
        anomalies=anomalies,
    )


# ---- URLごとの遷移 -------------------------------------------------------------


def _closed_date_for_new(prev: Optional[St], vt: str, today: date) -> str:
    """新規にclosedになった行の検知日。一度も確認できていない行(移行直後など)で
    valid_throughが過去日なら、その日(=実際に終わった日の近似)にして保持期限が不必要に伸びないようにする。"""
    if prev is not None and prev.last_seen:
        return iso(today)
    d = parse_date(vt)
    if d is not None and d < today:
        return iso(d)
    return iso(today)


def decide(
    key: str,
    source: str,
    prev: Optional[St],
    seen_shard: Optional[int],
    plan: Optional[SourcePlan],
    valid_through: str,
    today: date,
) -> St:
    """当日の状態。plan=None は seen_urls を出さない媒体(判定対象外)。"""
    today_s = iso(today)
    first = prev.first_seen if prev and prev.first_seen else today_s

    if seen_shard is not None:
        return St(source, first, today_s, str(seen_shard), "active", "seen", "", "0")

    last_seen = prev.last_seen if prev else ""
    last_shard = prev.last_shard if prev else ""
    misses = int(prev.misses or 0) if prev else 0

    def out(status, reason, closed="", n=misses):
        return St(source, first, last_seen, last_shard, status, reason, closed, str(n))

    if prev is not None and prev.status == "closed":
        return out("closed", prev.reason, prev.closed_date)

    judged = False
    reason = "no_seen_source"
    if plan is not None:
        reason = plan.reason
        if plan.mode == "judge":
            judged = True
        elif plan.mode == "partial":
            judged = bool(
                prev
                and prev.last_shard != ""
                and int(prev.last_shard) in plan.judged_shards
            )
            if not judged:
                reason = "shard_incomplete"

    vt = parse_date(valid_through)
    vt_past = vt is not None and vt < today

    if judged:
        # 完走した巡回で CLOSE_AFTER_MISSES 回続けて確認できなかったときだけ閉じる
        # (townwork/rikunabi 等は1日欠けて翌日戻る行が1〜4割ある)。valid_through が過去なら即閉じる。
        if not vt_past and misses + 1 < CLOSE_AFTER_MISSES:
            status = (
                "active" if prev is not None and prev.status == "active" else "unknown"
            )
            return out(status, "miss_pending", n=misses + 1)
        return out(
            "closed",
            "not_seen",
            _closed_date_for_new(prev, valid_through, today),
            misses + 1,
        )

    if vt_past:
        return out(
            "closed",
            "valid_through_expired",
            _closed_date_for_new(prev, valid_through, today),
        )

    ls = parse_date(last_seen)
    if prev is not None and prev.status == "active" and ls is not None:
        if (today - ls).days <= STALE_ACTIVE_DAYS:
            return out("active", reason)
        return out("unknown", "stale_unconfirmed")
    return out("unknown", reason)


def within_retention(st: St, today: date) -> bool:
    """closedは検知日から、unknownは最後に確認/初出した日から保持期間内か。activeは常に残す。"""
    if st.status == "active":
        return True
    if st.status == "closed":
        d = parse_date(st.closed_date)
        return d is None or d >= today - timedelta(days=CLOSED_RETENTION_DAYS)
    d = parse_date(st.last_seen) or parse_date(st.first_seen)
    return d is None or d >= today - timedelta(days=UNKNOWN_RETENTION_DAYS)


def prev_active_counts(prev: Dict[str, St]) -> tuple[Counter, Dict[str, Counter]]:
    """前日activeの (媒体別件数, 媒体別のシャード別件数)。安全装置の分母。"""
    by_src: Counter = Counter()
    by_shard: Dict[str, Counter] = {}
    for st in prev.values():
        if st.status == "active":
            by_src[st.source] += 1
            by_shard.setdefault(st.source, Counter())[st.last_shard] += 1
    return by_src, by_shard


class Judge:
    """当日の判定器。prev(前回状態)とseenから媒体ごとの計画を立て、行ごとに decide する。"""

    def __init__(
        self,
        prev: Dict[str, St],
        seen_by_source: Dict[str, SourceSeen],
        closure_sources: Iterable[str],
        today: date,
        ratio: float = SAFETY_THRESHOLD_RATIO,
    ):
        self.prev = prev
        self.today = today
        self.vt_unreliable: set = set()
        self.plans: Dict[str, SourcePlan] = {}
        self._url_shard: Dict[str, Dict[str, int]] = {}
        by_src, by_shard = prev_active_counts(prev)
        for src in closure_sources:
            seen = seen_by_source.get(src)
            stab = shard_stability(prev, seen) if seen and seen.missing else None
            self.plans[src] = plan_source(
                src, seen, by_src.get(src, 0), by_shard.get(src, Counter()), stab, ratio
            )
            self._url_shard[src] = seen.url_shard() if seen else {}

    def vt_past_rate(
        self, source: str, key: str, valid_through: str, acc: Counter
    ) -> None:
        """1周目で呼ぶ。当日確認できた行の valid_through が過去だった割合を媒体ごとに数える。"""
        if key in self._url_shard.get(source, ()):
            acc[(source, "seen")] += 1
            vt = parse_date(valid_through)
            if vt is not None and vt < self.today:
                acc[(source, "past")] += 1

    def set_vt_reliability(self, acc: Counter) -> set:
        """確認できた行の valid_through が過去ばかりの媒体(掲載日からの固定日数や取得時の値のまま更新されない)は、
        valid_through を判定に使わない。"""
        self.vt_unreliable = {
            src
            for (src, kind), n in acc.items()
            if kind == "seen" and acc[(src, "past")] / n > VT_UNRELIABLE_RATIO
        }
        return self.vt_unreliable

    def decide_row(self, key: str, source: str, valid_through: str) -> St:
        if source in self.vt_unreliable:
            valid_through = ""
        plan = self.plans.get(source)
        sh = self._url_shard.get(source, {}).get(key) if plan else None
        return decide(
            key, source, self.prev.get(key), sh, plan, valid_through, self.today
        )


# ---- 状態CSV -----------------------------------------------------------------


def write_state(f, state: Dict[str, St]) -> None:
    w = csv.writer(f)
    w.writerow(STATE_COLUMNS)
    for k, s in state.items():
        w.writerow([k, *s])


def read_state(lines: Iterable[str]) -> Dict[str, St]:
    out: Dict[str, St] = {}
    r = csv.reader(lines)
    header = next(r, None)
    if header is None:
        return out
    header[0] = header[0].lstrip("﻿")
    if header != STATE_COLUMNS:
        raise ValueError(f"job_state のヘッダが想定と違います: {header}")
    for row in r:
        if len(row) == len(STATE_COLUMNS):
            out[row[0]] = St(*row[1:])
    return out
