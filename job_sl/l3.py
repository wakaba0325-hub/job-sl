"""L3: 全ソースL2 → job_master 統合。

company_master突合ロジック:

- 正規化キーは company_master の `商号_nor` と同一の規則(`company-norm`パッケージの
  `core_key`をvendor化)。法人格36種+異体字統一+NFKC小文字化まで揃える(旧実装は
  株式会社等5種のみ剥がす簡易版で、医療法人/学校法人/士業法人等を挟むと未マッチ・
  誤あいまい化の原因になっていた)。
- 正規化キーだけでは同名多数(あいまい)が増えるため、以下を順に適用して一意化を試みる:
    1. 処理区分(国税庁法人番号公表データの処理区分コード)が閉鎖・取消系の候補を除外
    2. それでも複数残る場合、求人の勤務地(location)から都道府県を抽出し、
       本店所在都道府県が一致する候補に絞り込む
  どちらも効かなければ `is_ambiguous_company=1` のまま(法人番号は空)。
- 完全未マッチの場合のみ、注記("(旧名：〜)"等)除去・拠点表記("〜支店"等)除去した
  キーで再突合を試みる(過剰な正規化による誤結合を避けるため、一次キーで見つかった
  場合はこのフォールバックを使わない)。
- 採用した解決方法は `match_method` に記録する
  (exact / closure_excluded / pref_disambiguated / note_stripped / branch_stripped)。
- 新規掲載判定は job_url を前回 job_master と比較する日次差分(is_new_today)。
"""

from __future__ import annotations

import re
import unicodedata

_DASH_RE = re.compile(r"[‐‑‒–—―−﹣－]")


def _unify_dash(s: str) -> str:
    return _DASH_RE.sub("-", s or "")


_LEGAL_FORMS = [
    "株式会社",
    "有限会社",
    "合同会社",
    "合資会社",
    "合名会社",
    "一般社団法人",
    "公益社団法人",
    "一般財団法人",
    "公益財団法人",
    "特定非営利活動法人",
    "npo法人",
    "医療法人社団",
    "医療法人財団",
    "医療法人",
    "社会福祉法人",
    "学校法人",
    "宗教法人",
    "農事組合法人",
    "事業協同組合",
    "協同組合",
    "労働組合",
    "農業協同組合",
    "漁業協同組合",
    "信用金庫",
    "信用組合",
    "相互会社",
    "独立行政法人",
    "地方独立行政法人",
    "国立大学法人",
    "公立大学法人",
    "特殊法人",
    "認可法人",
    "管理組合法人",
    "税理士法人",
    "弁護士法人",
    "監査法人",
    "(株)",
    "(有)",
    "(合)",
    "(同)",
]
_FORMS_SORTED = sorted(_LEGAL_FORMS, key=len, reverse=True)

_ITAIJI = {
    "學": "学",
    "國": "国",
    "會": "会",
    "體": "体",
    "應": "応",
    "髙": "高",
    "﨑": "崎",
    "澤": "沢",
    "齋": "斎",
    "齊": "斉",
    "邊": "辺",
    "邉": "辺",
    "濱": "浜",
    "廣": "広",
    "藪": "薮",
    "籔": "薮",
    "驒": "騨",
}


def core_key(name: str) -> str:
    """company_master の商号_norと同一規則で正規化(company-normパッケージのvendor版)。"""
    if not name:
        return ""
    s = _unify_dash(unicodedata.normalize("NFKC", str(name).strip()).lower())
    if not s:
        return ""
    for f in _FORMS_SORTED:
        e = re.escape(f)
        s = re.sub(f"^{e}\\s*", "", s)
        s = re.sub(f"\\s*{e}$", "", s)
    s = s.replace("・", "")
    for o, n in _ITAIJI.items():
        s = s.replace(o, n)
    return s.strip()


# 旧公開名を維持(他コードからの参照に備える)
company_core = core_key

# 国税庁法人番号公表データの処理区分: 71/72/81 は法人番号指定の取消・職権閉鎖等
_CLOSED_SHOBUN_CODES = {"71", "72", "81"}

_PREFS = [
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
]
_PREF_RE = re.compile("|".join(_PREFS))

_NOTE_RE = re.compile(r"【[^】]*】|\([^)]*\)|（[^）]*）|/.*$|\|.*$")
_BRANCH_SUFFIX_RE = re.compile(
    r"(本社|東京本社|.{0,6}?(支店|支社|営業所|事業所|事業部|工場|支局|センター|オフィス|店))$"
)

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


def index_company_master(
    cm_rows,
    shogo_col: str = "商号",
    houjin_col: str = "法人番号",
    pref_col: str = "都道府県",
    shobun_col: str = "処理区分",
):
    """company_master行(iterable of dict) → {core_key: [(法人番号, 都道府県, 処理区分), ...]}。"""
    idx: dict = {}
    for r in cm_rows:
        h = (r.get(houjin_col) or "").strip()
        if not h:
            continue
        key = core_key(r.get(shogo_col, ""))
        if not key:
            continue
        idx.setdefault(key, []).append(
            (h, (r.get(pref_col) or "").strip(), (r.get(shobun_col) or "").strip())
        )
    return idx


def _resolve(cands: list[tuple[str, str, str]], location: str):
    """候補リストから一意化を試みる。戻り値は (法人番号, is_ambiguous, match_method)。"""
    active = [c for c in cands if c[2] not in _CLOSED_SHOBUN_CODES]
    pool = active if active else cands
    uniq = sorted({c[0] for c in pool})
    if len(uniq) == 1:
        method = "closure_excluded" if len(pool) < len(cands) else "exact"
        return uniq[0], False, method

    m = _PREF_RE.search(location or "")
    if m:
        narrowed = sorted({c[0] for c in pool if c[1] == m.group(0)})
        if len(narrowed) == 1:
            return narrowed[0], False, "pref_disambiguated"

    return "", True, "ambiguous"


def match_houjin(company_name: str, index: dict, location: str = ""):
    """(法人番号 or "", 同名複数で未解決か, match_method) を返す。"""
    key = core_key(company_name)
    cands = index.get(key)
    if cands:
        return _resolve(cands, location)

    # 完全未マッチ時のみ、注記除去→拠点表記除去の順でフォールバック
    s = unicodedata.normalize("NFKC", str(company_name or "").strip())
    s = _NOTE_RE.sub("", s)
    note_key = core_key(s)
    if note_key and note_key != key:
        cands = index.get(note_key)
        if cands:
            houjin, ambiguous, method = _resolve(cands, location)
            return houjin, ambiguous, ("note_stripped" if method == "exact" else method)

    branch_key = _BRANCH_SUFFIX_RE.sub("", key) if key else ""
    if branch_key and branch_key != key:
        cands = index.get(branch_key)
        if cands:
            houjin, ambiguous, method = _resolve(cands, location)
            return (
                houjin,
                ambiguous,
                ("branch_stripped" if method == "exact" else method),
            )

    return "", False, "unmatched"
