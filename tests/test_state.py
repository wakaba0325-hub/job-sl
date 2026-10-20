from job_sl.state import load_state, save_state, state_key
from job_sl.l2_stream import l1_keys


def test_roundtrip_and_missing(s3):
    assert load_state("src") == {}
    save_state("src", {"next_door": 12, "last_door": "https://x/あ/"})
    assert load_state("src") == {"next_door": 12, "last_door": "https://x/あ/"}


def test_state_file_is_not_read_as_l1(s3):
    save_state("src", {"a": 1})
    s3.objects["raw/job_sources/src/20261001/src.csv"] = b"job_url\r\n"
    assert l1_keys("src") == ["raw/job_sources/src/20261001/src.csv"]
    assert "/_state/" in state_key("src")
