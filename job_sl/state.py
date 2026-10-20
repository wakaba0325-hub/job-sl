"""コレクタの再開用チェックポイント(JSON)。

`raw/job_sources/<source>/_state/run_state.json` に保存する。パス中の `/_` は L1 の読み出し
(`l1_keys` / `read_all_l1`)から除外される規則に合致するので、L1 の中身には混ざらない。
1 run の取得が日次ウィンドウに収まらない媒体で、翌日以降に続きから巡回するために使う。
"""

from __future__ import annotations

import json

import job_sl as _core


def state_key(source: str) -> str:
    return f"raw/job_sources/{source}/_state/run_state.json"


def load_state(source: str) -> dict:
    try:
        body = _core._s3().get_object(Bucket=_core.BUCKET, Key=state_key(source))[
            "Body"
        ]
        return json.loads(body.read().decode("utf-8"))
    except Exception:
        return {}


def save_state(source: str, state: dict) -> None:
    _core._s3().put_object(
        Bucket=_core.BUCKET,
        Key=state_key(source),
        Body=json.dumps(state, ensure_ascii=False).encode("utf-8"),
    )
