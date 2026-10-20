"""世代の保持: 日付スナップショットは直近N世代だけ残す(news-sl.retention と同じ件数基準)。

件数基準なので、パイプラインが止まっても最新版は消えない。対象は `\\d{8}/` の日付フォルダのみ
(`_raw/` `_backup/` 等には触れない)。既定は dry-run。バケットはバージョニング有効のため、削除は
delete marker になり非現行版はライフサイクルで失効するまで復元できる。

保持世代数の根拠:
  job_master 7  1世代が重い(重複排除後でも数GB)。SalesPilotの同期は最新のみ読む。障害調査・巻き戻しに
                1週間あれば足りる。97世代=692GBの現状から約1/14。
  job_state  14 1世代が小さい(数百MB)。状態は前日分だけで遷移するが、判定ミスの遡り調査用にニュースと同じ14。
  L2         14 ソース別の累積スナップショット(最新が全件)。壊れた巡回の巻き戻し用にニュースと同じ14。
"""

from __future__ import annotations

import os
import re

from . import BUCKET, _s3

KEEP_JOB_MASTER = 7
KEEP_JOB_STATE = 14
KEEP_L2 = 14
MAX_DELETE_GENERATIONS = int(os.environ.get("JOB_PRUNE_MAX_GENERATIONS", "3"))


def prune_generations(
    prefix: str,
    fname: str,
    keep: int,
    max_delete: int = MAX_DELETE_GENERATIONS,
    dry_run: bool = True,
) -> list[str]:
    """`{prefix}{YYYYMMDD}/` のうち新しい順で keep+1 番目以降の日付フォルダを(古いものから最大
    max_delete 世代)削除する。最新日付フォルダに fname が無い/空のときは何も消さない。
    戻り値は削除(dry_runなら削除予定)のキー一覧。"""
    s3 = _s3()
    pat = re.compile(rf"^{re.escape(prefix)}(\d{{8}})/")
    by_date: dict[str, list[tuple[str, int]]] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix
    ):
        for o in page.get("Contents", []):
            m = pat.match(o["Key"])
            if m:
                by_date.setdefault(m.group(1), []).append((o["Key"], o["Size"]))
    dates = sorted(by_date, reverse=True)
    if len(dates) <= keep:
        print(f"[prune] {prefix}: {len(dates)}世代 <= {keep}、削除なし")
        return []
    latest = {k: size for k, size in by_date[dates[0]]}.get(
        f"{prefix}{dates[0]}/{fname}"
    )
    if not latest:
        print(f"[prune] {prefix}: 最新 {dates[0]} に {fname} が無い/空のため削除しない")
        return []
    victims = sorted(dates[keep:])[:max_delete]
    keys = [k for d in victims for k, _ in by_date[d]]
    print(
        f"[prune] {prefix}: {'dry-run 削除予定' if dry_run else '削除'} {victims} ({len(keys)}オブジェクト)"
    )
    if not dry_run:
        for i in range(0, len(keys), 1000):
            s3.delete_objects(
                Bucket=BUCKET,
                Delete={
                    "Objects": [{"Key": k} for k in keys[i : i + 1000]],
                    "Quiet": True,
                },
            )
    return keys
