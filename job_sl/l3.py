"""L3: 全ソースL2 → job_master 統合。

company_master突合ロジックは keyman-sl の l3.py(company_core/has_legal相当)を参考に
job-sl内で独立実装(keyman-slはprivateリポでDockerビルド時のgit認証問題を再発させないため
依存を持たない)。

- company_name を company_master の商号_nor と同じ規則(法人格除去+記号除去)で正規化し突合。
- 複数法人がヒットする場合は法人番号を空にし `is_ambiguous_company` を立てる(重複複製はしない、
  求人は人物と違い1行=1求人のため)。
- 新規掲載判定は job_url を前回 job_master と比較する日次差分(is_new_today)。
"""

from __future__ import annotations

import re
import unicodedata

_CORP_SUFFIXES = [
    "株式会社",
    "有限会社",
    "合同会社",
    "合資会社",
    "合名会社",
]

_STRIP_RE = re.compile(r"[\s・\-－ー（）()\.,、。]")

JOB_MASTER_SCHEMA = [
    "job_url",
    "source",
    "company_name",
    "houjin_bangou",
    "is_ambiguous_company",
    "job_title",
    "occupational_category",
    "employment_type",
    "salary_min",
    "salary_max",
    "salary_currency",
    "location",
    "work_hours",
    "required_skills",
    "benefits",
    "appeal_points",
    "remote_work",
    "company_industry",
    "company_size",
    "company_capital",
    "direct_apply",
    "posted_date",
    "valid_through",
    "description_snippet",
    "scraped_at",
    "first_seen_date",
    "is_new_today",
]


def company_core(name: str) -> str:
    """company_master の商号_norと同じ規則で正規化(法人格除去+記号除去+NFKC)。"""
    s = unicodedata.normalize("NFKC", name or "")
    for suf in _CORP_SUFFIXES:
        s = s.replace(suf, "")
    s = _STRIP_RE.sub("", s)
    return s.lower()


def index_company_master(
    cm_rows, shogo_col: str = "商号", houjin_col: str = "法人番号"
):
    """company_master行(iterable of dict) → {core_name: [法人番号,...]}。"""
    idx: dict = {}
    for r in cm_rows:
        h = (r.get(houjin_col) or "").strip()
        if not h:
            continue
        core = company_core(r.get(shogo_col, ""))
        if not core:
            continue
        idx.setdefault(core, []).append(h)
    return idx


def match_houjin(company_name: str, index: dict) -> tuple[str, bool]:
    """(法人番号 or "", 同名複数ヒットか)。"""
    core = company_core(company_name)
    hs = index.get(core)
    if not hs:
        return "", False
    uniq = sorted(set(hs))
    if len(uniq) == 1:
        return uniq[0], False
    return "", True
