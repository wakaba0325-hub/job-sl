"""L2 行の異常判定(build_l3 と backfill の共通)。

L1 は原本として不変なので、不正な行は L2 → L3 に入る前にここで除外する。L2 の build_l2 は
company_name 空を捨てるが、URL 不正や未来日付の scraped_at は通る。
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional

URL_RE = re.compile(r"^https?://[^\s/?#]+\S*$", re.IGNORECASE)
FUTURE_TOLERANCE_DAYS = 1  # JST/UTCのずれ分


def row_issue(r: dict, today: date) -> Optional[str]:
    """除外すべき行の理由コード。問題なければ None。"""
    url = (r.get("job_url") or "").strip()
    if not url:
        return "no_url"
    if not URL_RE.match(url):
        return "bad_url"
    if not (r.get("company_name") or "").strip():
        return "no_company"
    sa = (r.get("scraped_at") or "")[:10]
    if sa:
        try:
            if date.fromisoformat(sa) > today + timedelta(days=FUTURE_TOLERANCE_DAYS):
                return "future_scraped_at"
        except ValueError:
            return "bad_scraped_at"
    return None


def filled_count(r: dict) -> int:
    """同一URLの重複行のうち残す行を決める補助(scraped_at が同じなら情報が多い方)。"""
    return sum(1 for v in r.values() if v)


PLACEHOLDER_VALID_THROUGH_DAYS = 730  # 取得日からこれ以上先の期限は媒体の仮値(例: mynavi_pharma は掲載日+20年, hupro は 9999-12-31)


def normalize_valid_through(valid_through: str, scraped_at: str) -> str:
    """取得日から2年超先の valid_through は媒体の仮値なので空にする(「期限未確認」と同義)。解釈できない値はそのまま。"""
    try:
        vt = date.fromisoformat((valid_through or "")[:10])
        sa = date.fromisoformat((scraped_at or "")[:10])
    except ValueError:
        return valid_through or ""
    if (vt - sa).days > PLACEHOLDER_VALID_THROUGH_DAYS:
        return ""
    return valid_through
