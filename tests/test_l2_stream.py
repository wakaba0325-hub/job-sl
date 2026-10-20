import csv
import io
import random

import job_sl
from job_sl import COMMON_SCHEMA, key_for, to_common


def _csv_bytes(rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=COMMON_SCHEMA, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def _legacy_build_l2(rows_in_order):
    """変更前の build_l2 の中身(メモリ全展開版)。出力同値性の基準。"""
    commons = [to_common("src", r) for r in rows_in_order]
    commons = [c for c in commons if c["job_url"] and c["company_name"]]
    groups = {}
    for c in commons:
        groups.setdefault(key_for(c), []).append(c)
    out = []
    for items in groups.values():
        items.sort(key=lambda c: c["scraped_at"])
        base = dict(items[-1])
        for col in COMMON_SCHEMA:
            if not base.get(col):
                for c in reversed(items):
                    if c.get(col):
                        base[col] = c[col]
                        break
        out.append(base)
    return out


def _row(i, day, **kw):
    r = {
        "source": "src",
        "company_name": f"社{i % 7}",
        "job_title": f"職{i}",
        "job_url": f"https://ex.test/j/{i % 11}",
        "scraped_at": f"2026-10-{day:02d}T01:00:00+09:00",
        "description_snippet": '改行\r\nを含む,カンマ"引用"',
    }
    r.update(kw)
    return r


def _load_l2(s3, key):
    text = s3.objects[key].decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def test_streaming_matches_legacy_in_time_order(s3):
    rng = random.Random(1)
    parts = {}
    ordered = []
    for day in range(1, 8):
        rows = []
        for _ in range(25):
            i = rng.randrange(40)
            kw = {}
            if rng.random() < 0.4:
                kw["salary_min"] = str(rng.randrange(300, 900))
            if rng.random() < 0.1:
                kw["company_name"] = ""
            rows.append(_row(i, day, **kw))
        parts[f"raw/job_sources/src/2026100{day}/src.csv"] = _csv_bytes(rows)
        ordered.extend(rows)
    s3.objects.update(parts)
    s3.objects["raw/job_sources/src/20261001/src_seen_urls.csv"] = b"job_url\r\nx\r\n"

    _, key = job_sl.build_l2("src")
    got = _load_l2(s3, key)
    want = _legacy_build_l2(ordered)
    assert key == "master/job_sources/src/20261020/src.csv"
    assert [{c: g[c] for c in COMMON_SCHEMA} for g in got] == [
        {c: w[c] for c in COMMON_SCHEMA} for w in want
    ]


def test_l1_keys_skip_underscore_dirs_and_seen_files(s3):
    s3.objects["raw/job_sources/src/20261001/src.csv"] = _csv_bytes([_row(1, 1)])
    s3.objects["raw/job_sources/src/_raw/src.csv"] = _csv_bytes([_row(2, 1)])
    s3.objects["raw/job_sources/src/20261001/src_seen_urls.csv"] = b"job_url\r\n"
    from job_sl.l2_stream import l1_job_url_keys

    assert l1_job_url_keys("src") == {"https://ex.test/j/1"}


def test_existing_urls_streams_latest_l2(s3):
    s3.objects["master/job_sources/src/20261001/src.csv"] = _csv_bytes([_row(1, 1)])
    s3.objects["master/job_sources/src/20261002/src.csv"] = _csv_bytes(
        [_row(2, 2), _row(3, 2)]
    )
    assert job_sl.existing_urls("src") == {
        "https://ex.test/j/2",
        "https://ex.test/j/3",
    }


def test_write_false_returns_rows(s3):
    s3.objects["raw/job_sources/src/20261001/src.csv"] = _csv_bytes([_row(1, 1)])
    out, key = job_sl.build_l2("src", write=False)
    assert len(out) == 1 and key not in s3.objects
