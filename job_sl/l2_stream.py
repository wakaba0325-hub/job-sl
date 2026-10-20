"""L1/L2 をメモリに全展開せずに読み書きするための土台。

従来の `read_all_l1` + `build_l2` は、そのソースの L1 全履歴を `list[dict]` に展開し、
さらに正規化コピー・グループ化・出力・CSV文字列・エンコード済みbytes を同時に保持していた。
L1 が数百MB〜数GBに育ったシャード(kaigojob / mynavi / mynavi_baito)では
Fargate の 2〜4GB を超えて OOM Kill され、run の最後の build_l2 で L2 が出ないまま
前日の L2 に固定されていた。ここでは次の3点でピークメモリを「L2 の件数ぶんの行」に抑える。

- S3 を1行ずつ読み(`iter_csv_s3`)、全件リストを作らない
- job_url ごとに最新1行だけを保持しながらマージする(従来の「scraped_at 最新を採用、空列は
  古い行から補完」と同じ規則。L1 は日付昇順に読むので scraped_at は通常単調増加)
- 出力は一時ファイルへ逐次書き込み、`upload_file`(マルチパート)でアップロードする
"""

from __future__ import annotations

import csv
import os
import tempfile
from typing import Iterator

import job_sl as _core

_L1_SKIP = "/_"


def iter_csv_s3(key: str) -> Iterator[dict]:
    """S3上のCSVを全量をメモリに載せず1行ずつ dict で返す。"""
    return csv.DictReader(_core.iter_lines_s3(key))


def l1_keys(source: str) -> list[str]:
    """そのソースのL1パーティション(`<source>.csv`)のキーを日付昇順で返す。"""
    s3 = _core._s3()
    keys = []
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=_core.BUCKET, Prefix=f"raw/job_sources/{source}/"):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith(f"/{source}.csv") and _L1_SKIP not in k:
                keys.append(k)
    return sorted(keys)


def iter_l1_rows(source: str) -> Iterator[dict]:
    """全L1パーティションの行を日付昇順で1行ずつ返す(`read_all_l1`のストリーム版)。"""
    for k in l1_keys(source):
        yield from iter_csv_s3(k)


def l1_job_url_keys(source: str) -> set[str]:
    """L1全履歴の job_url 正規化キー集合。行は保持しない。"""
    out = set()
    for r in iter_l1_rows(source):
        k = _core.key_for(_core.to_common(source, r))
        if k:
            out.add(k)
    return out


def _merge(cur: list[str], new: list[str], ts_idx: int) -> list[str]:
    """同一 job_url の2行をマージ。scraped_at が新しい(同値なら後に読んだ)方を基準にし、
    基準の空列は他方の値で補完する。"""
    if new[ts_idx] >= cur[ts_idx]:
        base, other = new, cur
    else:
        base, other = cur, new
    for i, v in enumerate(base):
        if not v and other[i]:
            base[i] = other[i]
    return base


def merge_l1(source: str) -> dict[str, list[str]]:
    """L1全履歴を job_url ごとに1行へマージ。値は COMMON_SCHEMA 順の文字列リスト。
    company_name または job_url が空の行は除外(従来の build_l2 と同じ)。"""
    cols = _core.COMMON_SCHEMA
    i_ts = cols.index("scraped_at")
    best: dict[str, list[str]] = {}
    for r in iter_l1_rows(source):
        c = _core.to_common(source, r)
        if not c["job_url"] or not c["company_name"]:
            continue
        vals = [c[col] for col in cols]
        k = _core.key_for(c)
        cur = best.get(k)
        best[k] = vals if cur is None else _merge(cur, vals, i_ts)
    return best


def build_l2_stream(source: str, run_date: str | None = None, write: bool = True):
    """`build_l2` のメモリ効率版。戻り値は (dictのリスト or [], L2キー)。
    write=True のときは出力を一時ファイル経由でS3へ上げ、行リストは返さない(空リスト)。"""
    best = merge_l1(source)
    rd = run_date or _core._today()
    key = f"master/job_sources/{source}/{rd}/{source}.csv"
    cols = _core.COMMON_SCHEMA
    if not write:
        return [dict(zip(cols, v)) for v in best.values()], key

    fd, path = tempfile.mkstemp(suffix=".csv")
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for v in best.values():
                w.writerow(v)
        _core._s3().upload_file(path, _core.BUCKET, key)
    finally:
        os.unlink(path)
    return [], key
