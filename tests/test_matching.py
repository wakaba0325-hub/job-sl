from job_sl import matching as m


def cand(h, shogo, pref="東京都", shobun="01", city="", doms="", emp=0):
    return (h, pref, shobun, shogo, city, doms, emp)


def index(*cands):
    idx = {}
    for c in cands:
        idx.setdefault(m.core_key(c[3]), []).append(c)
    return idx


def test_core_key_absorbs_spaces_dots_width_case():
    assert (
        m.core_key("株式会社 TENRYU") == m.core_key("株式会社ＴＥＮＲＹＵ") == "tenryu"
    )
    assert m.core_key("ルイ･ヴィトン ジャパン株式会社") == m.core_key(
        "ルイ・ヴィトンジャパン(株)"
    )
    assert m.core_key("認定NPO法人 ふれあい") == "ふれあい"


def test_exact_and_legal_form_filtered():
    idx = index(cand("1", "株式会社ABC"), cand("2", "有限会社ABC"))
    assert m.match_houjin("株式会社abc", idx)[:3] == ("1", False, "legal_form_filtered")
    one = index(cand("1", "株式会社ABC"))
    assert m.match_houjin("ABC", one)[:3] == ("1", False, "exact")


def test_legal_form_mismatch_is_not_matched():
    idx = index(cand("9", "九州電力労働組合"))
    assert m.match_houjin("九州電力株式会社", idx)[:3] == (
        "",
        False,
        "legal_form_mismatch",
    )


def test_merger_code_is_not_treated_as_closed_by_default():
    idx = index(cand("1", "株式会社X", shobun="71"), cand("2", "株式会社X"))
    assert m.match_houjin("株式会社X", idx)[:3] == ("", True, "ambiguous")


def test_closure_excluded_when_codes_given(monkeypatch):
    monkeypatch.setattr(m, "_CLOSED_SHOBUN_CODES", {"71"})
    idx = index(cand("1", "株式会社X", shobun="71"), cand("2", "株式会社X"))
    assert m.match_houjin("株式会社X", idx)[:3] == ("2", False, "closure_excluded")


def test_dominant_size_needs_exactly_one_large_candidate():
    idx = index(
        cand("1", "株式会社X", emp=5000),
        cand("2", "株式会社X"),
        cand("3", "株式会社X", emp=3),
    )
    assert m.match_houjin("株式会社X", idx)[:3] == ("1", False, "dominant_size")
    two = index(cand("1", "株式会社X", emp=5000), cand("2", "株式会社X", emp=300))
    assert m.match_houjin("株式会社X", two)[:3] == ("", True, "ambiguous")


def test_shogo_exact_prefers_same_form_position():
    idx = index(cand("1", "ABC株式会社"), cand("2", "株式会社ABC"))
    assert m.match_houjin("株式会社ABC", idx)[:3] == ("2", False, "shogo_exact")


def test_domain_then_city_disambiguation():
    a = cand("A", "株式会社X", "東京都", city="渋谷区", doms="x-a.co.jp")
    b = cand("B", "株式会社X", "大阪府", city="大阪市北区", doms="x-b.co.jp")
    idx = index(a, b)
    r = m.match_houjin(
        "X", idx, location="東京都新宿区", text="公式 https://www.x-b.co.jp/recruit"
    )
    assert r[:3] == ("B", False, "domain_disambiguated")
    r = m.match_houjin("X", idx, location="大阪府大阪市北区梅田")
    assert r[:3] == ("A" if False else "B", False, "city_disambiguated")


def test_job_board_host_is_not_a_domain_hint():
    a = cand("A", "株式会社X", doms="jobs.example.co.jp")
    b = cand("B", "株式会社X", doms="other.co.jp")
    r = m.match_houjin(
        "X",
        index(a, b),
        text="https://jobs.example.co.jp/a",
        job_url="https://jobs.example.co.jp/job/1",
    )
    assert r[:3] == ("", True, "ambiguous")


def test_pref_only_is_not_assigned_by_default():
    idx = index(cand("A", "株式会社X", "東京都"), cand("B", "株式会社X", "大阪府"))
    assert m.match_houjin("株式会社X", idx, location="東京都港区")[:3] == (
        "",
        True,
        "ambiguous",
    )
    r = m.match_houjin("株式会社X", idx, location="東京都港区", disabled=frozenset())
    assert r[:3] == ("A", False, "pref_disambiguated")


def test_token_extracted_from_facility_name():
    idx = index(cand("1", "合同会社蔵識"), cand("2", "グリーンライフ株式会社"))
    assert m.match_houjin("合同会社蔵識 はじめ訪問看護ステーション", idx)[:3] == (
        "1",
        False,
        "token_extracted",
    )
    assert m.match_houjin("グリーンライフ株式会社 グリーンライフ蕨", idx)[:3] == (
        "2",
        False,
        "token_extracted",
    )
    assert (
        m.match_houjin("株式会社 蔵識 本店", index(cand("1", "株式会社蔵識")))[2]
        == "token_extracted"
    )


def test_legal_form_tokens():
    assert (
        m.legal_form_tokens("合同会社蔵識 はじめ訪問看護ステーション")[-1]
        == "合同会社蔵識"
    )
    assert m.legal_form_tokens("ABC 株式会社") == ["ABC 株式会社", "ABC株式会社"]
    assert m.legal_form_tokens("TAMAYA空港店(株式会社玉屋)") == ["株式会社玉屋"]
    assert m.legal_form_tokens("(株)ドットライン【ドットライングループ】") == [
        "株式会社ドットライン"
    ]
    assert m.legal_form_tokens("ただの施設名") == []


def test_token_does_not_take_second_half_of_english_name():
    assert (
        m.legal_form_tokens("Mother's Industry株式会社")[0]
        == "Mother's Industry株式会社"
    )
    assert "Industry株式会社" not in m.legal_form_tokens("Mother's Industry株式会社")


def test_note_stripped():
    idx = index(cand("1", "株式会社ABC"))
    assert m.match_houjin("株式会社ABC【急募】", idx)[2] == "note_stripped"


def test_facility_prefix_takes_company_part_of_store_name():
    idx = index(cand("1", "株式会社クスリのアオキ", emp=17000))
    assert m.match_houjin("クスリのアオキ中町店", idx)[:3] == (
        "1",
        False,
        "facility_prefix",
    )
    assert m.match_houjin("クスリのアオキ 諏訪四賀店", idx)[2] == "facility_prefix"


def test_facility_prefix_needs_a_large_company():
    idx = index(cand("1", "有限会社すず薬局"))
    assert m.match_houjin("すず薬局 井口店", idx)[2] == "unmatched"


def test_facility_prefix_does_not_delete_middle_of_name():
    idx = index(cand("1", "株式会社幸"))
    assert m.match_houjin("幸楽苑 新潟五泉店", idx)[2] == "unmatched"


def test_disabled_method_leaves_ambiguous():
    idx = index(cand("1", "株式会社ABC"))
    r = m.match_houjin("株式会社ABC【急募】", idx, disabled={"note_stripped"})
    assert r[:3] == ("", True, "ambiguous")


def test_confidence_column():
    idx = index(cand("1", "株式会社ABC"))
    assert m.match_houjin("株式会社ABC", idx).confidence == "high"
    assert m.match_houjin("存在しない会社", idx).confidence == ""


def test_anonymous_and_staffing_flags():
    assert m.is_anonymous_name("社名非公開")
    assert m.is_anonymous_name("")
    assert m.is_anonymous_name("【社名非公開】東証一部上場の中華レストランチェーン企業")
    assert m.is_staffing_agent_name("株式会社スタッフサービス")
    assert m.is_staffing_agent_name("マンパワーグループ株式会社")
    assert not m.is_staffing_agent_name("トヨタ自動車株式会社")
