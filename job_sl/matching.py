"""求人の会社名 → company_master 照合(法人番号付与)。

news-sl の段階絞り込みを求人向けに移植したもの。社名から作る正規化キー(`core_key`)で
company_master の商号を引き、同名が複数ある場合は次の順に候補を絞る。
一意になった段の名前を `match_method` に記録する。

  0. 入力社名に法人格があれば、同種別の候補だけに絞る(legal_form_filtered)
  1. 閉鎖・取消系の処理区分の候補を除外(closure_excluded)
  2. 法人格つきの商号が入力社名と完全一致する候補が1件(shogo_exact)
  3. 求人本文中のURLドメインが company_master の domain / website_url と一致(domain_disambiguated)
  4. 求人の勤務地の市区町村が一致(city_disambiguated)
  5. 勤務地の都道府県だけで決める(pref_disambiguated)。精度が低いため既定OFF

社名欄に施設名・店舗名が混ざる求人(「合同会社蔵識 はじめ訪問看護ステーション」等)は、
全体で引けなかった場合に限り、法人格つきトークンを取り出して再照合する(token_extracted)。

DISABLED_METHODS に入っている手法で決まった行は、法人番号を付けず ambiguous として扱う
(誤付与より未付与を選ぶ)。評価で精度が低かった手法をここに足す。
"""

from __future__ import annotations

import re
import sys
import unicodedata
from typing import NamedTuple

_DASH_RE = re.compile(r"[‐‑‒–—―−﹣－]")

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
    "認定特定非営利活動法人",
    "特定非営利活動法人",
    "認定npo法人",
    "npo法人",
    "社会医療法人",
    "特定医療法人",
    "国立研究開発法人",
    "社団法人",
    "財団法人",
    "生活協同組合",
    "企業組合",
    "有限責任事業組合",
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
_FORM_ALT = "|".join(re.escape(f) for f in _FORMS_SORTED)
_FORM_PREFIX_RE = re.compile(f"^(?:{_FORM_ALT})")
_FORM_SUFFIX_RE = re.compile(f"(?:{_FORM_ALT})$")

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
_ITAIJI_TABLE = str.maketrans(_ITAIJI)
_WS_RE = re.compile(r"\s+")
_DOT_RE = re.compile("[・･·•∙⋅]")


def _base_norm(name) -> str:
    """NFKC・小文字・ダッシュ統一・空白と中点の除去・異体字統一(法人格は残す)。"""
    s = _DASH_RE.sub("-", unicodedata.normalize("NFKC", str(name or "")).lower())
    s = _WS_RE.sub("", s)
    s = _DOT_RE.sub("", s)
    return s.translate(_ITAIJI_TABLE)


def core_key(name) -> str:
    """商号の突合キー(法人格を除いた本体)。company_master 側も同じ関数で正規化する。"""
    s = _base_norm(name)
    if not s:
        return ""
    for _ in range(3):
        t = _FORM_SUFFIX_RE.sub("", _FORM_PREFIX_RE.sub("", s))
        if t == s:
            break
        s = t
    return s


_ABBREV = {
    "(株)": "株式会社",
    "(有)": "有限会社",
    "(合)": "合資会社",
    "(同)": "合同会社",
}
_ABBREV_RE = re.compile("|".join(re.escape(k) for k in _ABBREV))


def shogo_key(name) -> str:
    """法人格を残したまま揺れ(空白・全半角・大小・中点・略称)だけ吸収した完全一致用キー。"""
    return _ABBREV_RE.sub(lambda m: _ABBREV[m.group(0)], _base_norm(name))


_FORM_FAMILY = {
    "(株)": "株式会社",
    "(有)": "有限会社",
    "(合)": "合資会社",
    "(同)": "合同会社",
    "医療法人社団": "医療法人",
    "医療法人財団": "医療法人",
    "社会医療法人": "医療法人",
    "特定医療法人": "医療法人",
    "一般社団法人": "社団法人",
    "公益社団法人": "社団法人",
    "一般財団法人": "財団法人",
    "公益財団法人": "財団法人",
    "特定非営利活動法人": "npo",
    "認定特定非営利活動法人": "npo",
    "認定npo法人": "npo",
    "npo法人": "npo",
}


def form_type(name) -> str:
    """社名の先頭/末尾にある法人格の種別(同系統は同一視)。無ければ空文字。"""
    s = _base_norm(name)
    m = _FORM_PREFIX_RE.match(s) or _FORM_SUFFIX_RE.search(s)
    if not m:
        return ""
    return _FORM_FAMILY.get(m.group(0), m.group(0))


# 閉鎖扱いにする処理区分。**空(閉鎖判定なし)**。
# 処理区分 71(吸収合併)は存続会社(ツクイ・MSD・KADOKAWA等)にも付いており、閉鎖の根拠にならない。
# 旧実装は 71/72/81 を閉鎖として除外し、同名の実在する大手ではなく小規模な別会社を掴んでいた。
_CLOSED_SHOBUN_CODES: set = set()

# 同名候補のうち従業員数が突出して大きい1社だけがいるとき、その会社とみなす閾値
DOMINANT_MIN_EMPLOYEES = 100

# 決まっても法人番号を付けない手法(評価で精度が低かったもの)
DISABLED_METHODS = {"pref_disambiguated"}

# 手法ごとの信頼度。high=ほぼ確定 / medium=要確認の余地あり / low=誤りが目立つ
METHOD_CONFIDENCE = {
    "exact": "high",
    "legal_form_filtered": "high",
    "closure_excluded": "high",
    "shogo_exact": "high",
    "domain_disambiguated": "high",
    "city_disambiguated": "medium",
    "dominant_size": "medium",
    "token_extracted": "medium",
    "note_stripped": "medium",
    "facility_prefix": "medium",
    "pref_disambiguated": "low",
}

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
# 施設名・店舗名の語尾。社名欄が「クスリのアオキ中町店」のように会社名+施設名のとき、
# 先頭側の会社名部分だけを取り出すのに使う
_FACILITY_TAIL_RE = re.compile(
    r"(本社|支店|支社|営業所|事業所|事業部|工場|支局|センター|オフィス|店|校|教室|院|薬局|病院|"
    r"クリニック|ステーション|ホーム|園|館|本部|営業部)$"
)
# 施設名から取り出す会社名部分の最短文字数(短い語は別会社と衝突しやすい)
FACILITY_PREFIX_MIN_LEN = 4
# 施設名の先頭だけで決める場合、複数拠点を持つ会社(=従業員数が一定以上)に限る。
# 小規模な同名会社(「個別指導」「ステーキ」「すず薬局」等)を掴む誤りを避ける
FACILITY_PREFIX_MIN_EMPLOYEES = 100

# ---- 求人本文からの手掛かり ---------------------------------------------------

_HOST_RE = re.compile(
    r"(?<![a-z0-9.\-])((?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?\.)+"
    r"(?:co\.jp|ne\.jp|or\.jp|ac\.jp|go\.jp|ed\.jp|lg\.jp|com|net|org|biz|info|jp|io|co|"
    r"shop|tokyo|ai|app|me|dev|tech|xyz|cloud|asia|life|jobs|site|online|store|work|"
    r"world|team|tv|cc|us|kr|cn|tw|sg|eu|de|uk|fr|vn)(?![a-z0-9\-]))"
)
# 求人本文に出てもその会社のサイトとは限らない共通ホスト
_SKIP_HOSTS = (
    "twitter.com",
    "x.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "youtu.be",
    "line.me",
    "note.com",
    "google.com",
    "goo.gl",
    "bit.ly",
    "amazon.co.jp",
    "amazon.com",
    "apple.com",
    "linkedin.com",
    "wikipedia.org",
    "tiktok.com",
    "yahoo.co.jp",
    "rakuten.co.jp",
    "github.com",
    "wantedly.com",
    "green-japan.com",
    "indeed.com",
)


def text_hosts(text: str, exclude_host: str = "") -> set[str]:
    """本文中のURL/ドメインのホスト名集合(www.除去・小文字)。求人媒体自身や共通サービスは除く。"""
    out = set()
    t = unicodedata.normalize("NFKC", text or "").lower()
    for m in _HOST_RE.finditer(t):
        h = m.group(1)
        h = h.removeprefix("www.")
        if exclude_host and (h == exclude_host or h.endswith("." + exclude_host)):
            continue
        if any(h == x or h.endswith("." + x) for x in _SKIP_HOSTS):
            continue
        out.add(h)
    return out


def _host_of(url: str) -> str:
    m = re.match(r"^[a-z]+://([^/:?#]+)", (url or "").strip().lower())
    if not m:
        return ""
    h = m.group(1)
    parts = h.split(".")
    # 求人媒体のサブドメイン(www.xxx.co.jp / jobs.xxx.com)を丸ごと除外できるよう登録ドメイン相当にする
    if len(parts) >= 3 and parts[-2] in ("co", "ne", "or", "ac", "go", "ed", "lg"):
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


# ---- 索引 ------------------------------------------------------------------

# 索引の候補タプル: (法人番号, 都道府県, 処理区分, 商号, 市区町村, ドメイン(空白区切り), 従業員数)
_H, _P, _S, _N, _C, _D, _E = range(7)


def _url_host(url: str) -> str:
    u = (url or "").strip().lower()
    if not u:
        return ""
    m = re.match(r"^(?:[a-z]+://)?([^/:?#\s]+)", u)
    h = m.group(1) if m else ""
    return h.removeprefix("www.")


def _to_int(v) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def index_company_master(rows):
    """company_master 行(dict の iterable)→ {core_key: [候補タプル, ...]}。

    使う列: 法人番号 / 商号 / 都道府県 / 市区町村 / 処理区分 / domain / website_url / 従業員数。
    """
    idx: dict = {}
    for r in rows:
        h = (r.get("法人番号") or "").strip()
        if not h:
            continue
        shogo = r.get("商号", "")
        key = core_key(shogo)
        if not key:
            continue
        doms = []
        for d in (
            (r.get("domain") or "").strip().lower(),
            _url_host(r.get("website_url")),
        ):
            if d and d not in doms:
                doms.append(d)
        idx.setdefault(key, []).append(
            (
                h,
                sys.intern((r.get("都道府県") or "").strip()),
                sys.intern((r.get("処理区分") or "").strip()),
                (shogo or "").strip(),
                sys.intern((r.get("市区町村") or "").strip()),
                " ".join(doms),
                _to_int(r.get("従業員数")),
            )
        )
    return idx


# ---- あいまい解消 -------------------------------------------------------------
# 各関数は候補リストを受け取り、手掛かりに合う候補だけを返す(合うものが無ければ空)。


def narrow_by_domain(pool: list, hosts: set[str]) -> list:
    if not hosts:
        return []
    out = []
    for c in pool:
        for d in c[_D].split():
            if any(h == d or h.endswith("." + d) for h in hosts):
                out.append(c)
                break
    return out


def narrow_by_city(pool: list, location: str) -> list:
    t = unicodedata.normalize("NFKC", location or "")
    prefs = set(_PREF_RE.findall(t))
    out = []
    for c in pool:
        city = c[_C]
        if len(city) < 2:
            continue
        if prefs and c[_P] not in prefs:
            continue
        # 「大阪市中央区」の中の「中央区」を東京の中央区と取り違えない
        pat = (
            "(?<!市)" if city.endswith("区") and "市" not in city else ""
        ) + re.escape(city)
        if re.search(pat, t):
            out.append(c)
    return out


def narrow_by_pref(pool: list, location: str) -> list:
    m = _PREF_RE.search(location or "")
    if not m:
        return []
    return [c for c in pool if c[_P] == m.group(0)]


def _distinct(pool: list) -> list:
    return sorted({c[_H] for c in pool})


class Match(NamedTuple):
    houjin: str
    is_ambiguous: bool
    method: str
    confidence: str


def _resolve(cands: list, ctx: dict, raw_name: str):
    """候補群から1社に絞る。戻り値 (法人番号, あいまい, method)。絞れなければ ("", True, "ambiguous")。
    法人格の種別が合う候補が1つも無い場合は None(呼び出し側が次の手段へ進む)。"""
    ftype = form_type(raw_name)
    formed = [c for c in cands if form_type(c[_N]) == ftype] if ftype else cands
    if not formed:
        return None
    pool = [c for c in formed if c[_S] not in _CLOSED_SHOBUN_CODES]
    if not pool:
        pool = formed

    uniq = _distinct(pool)
    if len(uniq) == 1:
        if len(pool) < len(formed):
            method = "closure_excluded"
        elif len(_distinct(cands)) > 1:
            method = "legal_form_filtered"
        else:
            method = "exact"
        return uniq[0], False, method

    k = shogo_key(raw_name)
    exact = sorted({c[_H] for c in pool if shogo_key(c[_N]) == k})
    if len(exact) == 1:
        return exact[0], False, "shogo_exact"

    stages = [
        ("domain_disambiguated", narrow_by_domain, ctx["hosts"]),
        ("city_disambiguated", narrow_by_city, ctx["location"]),
        ("pref_disambiguated", narrow_by_pref, ctx["location"]),
    ]
    for method, fn, arg in stages:
        narrowed = fn(pool, arg)
        if narrowed:
            pool = narrowed
            u = _distinct(pool)
            if len(u) == 1:
                return u[0], False, method

    big = sorted({c[_H] for c in pool if c[_E] >= DOMINANT_MIN_EMPLOYEES})
    if len(big) == 1:
        return big[0], False, "dominant_size"

    return "", True, "ambiguous"


# ---- 社名欄からの法人格つきトークン抽出 ----------------------------------------

_SEG_RE = re.compile(r"[/／|｜,、，:：;；「」『』【】\[\]<>＜＞()]+")
_WORD_RE = re.compile(r"\s+")


def _has_form_edge(w: str) -> bool:
    wl = w.lower()
    return bool(_FORM_PREFIX_RE.match(wl) or _FORM_SUFFIX_RE.search(wl))


def legal_form_tokens(name: str) -> list[str]:
    """社名欄から法人格を含む語を取り出す(施設名・店舗名・キャッチコピーを落とす)。

    「合同会社蔵識 はじめ訪問看護ステーション」→「合同会社蔵識」、
    「グリーンライフ株式会社 グリーンライフ蕨」→「グリーンライフ株式会社」、
    「TAMAYA空港店(株式会社玉屋)」→「株式会社玉屋」。
    括弧・スラッシュ等で区切った区間に法人格があればその区間全体(空白入りの社名用)を先に、
    次に空白区切りの語を候補にする。「株式会社 ABC」のように法人格だけの語は隣の語とつなぐ。
    """
    s = unicodedata.normalize("NFKC", str(name or "")).strip()
    for ab, full in _ABBREV.items():
        s = s.replace(ab, full)
    out: list[str] = []
    for seg in (x.strip() for x in _SEG_RE.split(s)):
        if not seg:
            continue
        if _has_form_edge(seg) and core_key(seg):
            out.append(seg)
        words = [w for w in _WORD_RE.split(seg) if w]
        for i, w in enumerate(words):
            if not _has_form_edge(w):
                continue
            if core_key(w):
                # 「Mother's Industry株式会社」のような英語の複数語社名の後ろ半分だけを取らない
                if i > 0 and words[i - 1][-1:].isascii() and w[:1].isascii():
                    continue
                out.append(w)
            elif _FORM_PREFIX_RE.match(w.lower()) and i + 1 < len(words):
                out.append(w + words[i + 1])
            elif i > 0:
                out.append(words[i - 1] + w)
    seen = set()
    return [t for t in out if not (t in seen or seen.add(t))]


# ---- 社名非公開・人材会社 ------------------------------------------------------

_ANONYMOUS_RE = re.compile(r"非公開|公開していません|エージェントにお問い合わせ|匿名")

# 求人媒体上で採用主体ではなく紹介・派遣元として載る会社(誤って「採用中の会社」扱いしない)
_STAFFING_RE = re.compile(
    r"人材|スタッフ(?:サービス|ィング)?|派遣|キャリアデザイン|転職|"
    r"マンパワー|ウィルオブ|ウイルオブ|アデコ|パソナ|テンプスタッフ|リクルート(?:スタッフィング)?|"
    r"エン・ジャパン|ジェイエイシー|ヒューマンリソシア|ヒューマンアカデミー|"
    r"メイテック|アウトソーシング|ワークポート|ランスタッド|ビズリーチ|"
    r"家庭教師のトライ|ヒューマントラスト|ヒューマンホールディングス"
)


def is_anonymous_name(name: str) -> bool:
    s = unicodedata.normalize("NFKC", str(name or "")).strip()
    return not s or bool(_ANONYMOUS_RE.search(s))


def is_staffing_agent_name(name: str) -> bool:
    s = unicodedata.normalize("NFKC", str(name or ""))
    return bool(_STAFFING_RE.search(s))


# ---- 本体 ------------------------------------------------------------------


def match_houjin(
    company_name: str,
    index: dict,
    location: str = "",
    text: str = "",
    job_url: str = "",
    disabled=None,
) -> Match:
    """求人1行の会社名から法人番号を引く。

    location は勤務地(市区町村・都道府県の絞り込み用)、text は求人本文など
    (ドメイン絞り込み用)、job_url は求人媒体自身のドメインを手掛かりから外すのに使う。
    disabled は DISABLED_METHODS の上書き(評価用)。
    """
    disabled = DISABLED_METHODS if disabled is None else disabled
    raw_name = (company_name or "").strip()
    ctx = {
        "location": location or "",
        "hosts": text_hosts(text, _host_of(job_url)),
    }

    def finish(houjin, ambiguous, method) -> Match:
        if houjin and method in disabled:
            return Match("", True, "ambiguous", "")
        return Match(
            houjin,
            ambiguous,
            method,
            METHOD_CONFIDENCE.get(method, "") if houjin else "",
        )

    mismatch = False

    def attempt(k: str, name_for_form: str, label: str, min_employees: int = 0):
        nonlocal mismatch
        cands = index.get(k) if k else None
        if not cands:
            return None
        r = _resolve(cands, ctx, name_for_form)
        if r is None:
            mismatch = True
            return None
        houjin, ambiguous, method = r
        if min_employees and not any(
            c[_H] == houjin and c[_E] >= min_employees for c in cands
        ):
            return None
        relabel = method in ("exact", "legal_form_filtered", "closure_excluded")
        return finish(houjin, ambiguous, label if relabel and label else method)

    key = core_key(raw_name)
    r = attempt(key, raw_name, "")
    if r:
        return r

    s = unicodedata.normalize("NFKC", raw_name)
    s = _NOTE_RE.sub("", s)
    note_key = core_key(s)
    if note_key and note_key != key:
        r = attempt(note_key, s, "note_stripped")
        if r:
            return r

    for tok in legal_form_tokens(raw_name):
        tk = core_key(tok)
        if tk and tk != key:
            r = attempt(tk, tok, "token_extracted")
            if r:
                return r

    if key and _FACILITY_TAIL_RE.search(key):
        for n in range(len(key) - 1, FACILITY_PREFIX_MIN_LEN - 1, -1):
            if key[:n] in index:
                r = attempt(
                    key[:n], key[:n], "facility_prefix", FACILITY_PREFIX_MIN_EMPLOYEES
                )
                if r:
                    return r
                break

    return Match("", False, "legal_form_mismatch" if mismatch else "unmatched", "")
