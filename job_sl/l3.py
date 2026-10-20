"""L3: 全ソースL2 → job_master 統合。

company_master突合ロジックは matching.py を参照。

- 新規掲載判定は job_url を前回 job_master と比較する日次差分(is_new_today)。
"""

from __future__ import annotations

JOB_MASTER_SCHEMA = [
    "job_url",
    "source",
    "company_name",
    "houjin_bangou",
    "is_ambiguous_company",
    "match_method",
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
    "listing_status",
    "closed_detected_date",
    "match_confidence",
    "is_staffing_agent",
    "is_anonymous",
]

# 掲載終了検知対象ソース: サイト全体を毎日フル巡回しており、当日確認できた全URL一覧
# (`{source}_seen_urls.csv`、jobmedley/mynaviはshard分割時 `{source}_seen_urls_shard{N}.csv`)を
# write_seen_urls で書き出している媒体のみ。mynaviは7シャード(mynavi0..6)をL3側で
# source="mynavi"として統合する既存パターンに合わせ、seen_urls側もshard0..6を
# source="mynavi"名義で書いて束ねる。
# rikunabiは検索結果ページングにrobots.txt Disallow対象の`?cursor=`を使う
# (rikunabi-job-collector側でユーザーが経緯を把握の上で無視する方針を明示決定済み。
# 詳細は rikunabi-job-collector リポジトリの README 参照)。
# 未対応(bizreach)は日次フル巡回コストの都合で今回のスコープ外のため、
# listing_statusは常に"unknown"とする(把握できないものをactiveと誤断定しない)。
CLOSURE_DETECTION_SOURCES = [
    "green",
    "paiza",
    "ambi",
    "jobmedley",
    "type",
    "youtrust",
    "mynavi",
    "wantedly",
    "rikunabi",
    "levtech_career",
    "levtech_freelance",
    "en_tenshoku",
    "mynavi_shinsotsu",
    "geekly",
    "jac_recruitment",
    "townwork",
    "kaigojob",
    "careerconnection",
    "findy",
    "hupro",
    "crowdworks",
    "itpropartners",
    "mynavi_kango",
    "mynavi_pharma",
    "mynavi_comedical",
    "mynavi_kaigo",
    "mynavi_hoiku",
    "mynavi_zeirishi",
    "mynavi_kaikeishi",
    "levwell_kango",
    "levwell_kaigo",
]
# 未登録(意図的): ms_japan(検索結果件数が不安定と判明・原因特定まで保留)、
# stanby(都道府県トップページ上位分のみのサンプリング収集のため「今日見えない=掲載終了」の前提が成立しない)、
# mynavi_baito(「ドア」ページ1件あたり先頭30件までしか拾えない構造上の網羅性制約があり、
# 「今日見えない=掲載終了」の前提が成立しない)。

# 安全装置の閾値: 当日確認できたURL件数が前日job_master内の同ソース件数の
# この比率を下回ったら、クロール不完全の疑いありとして当日の掲載終了判定を全件スキップする。
SAFETY_THRESHOLD_RATIO = 0.7
