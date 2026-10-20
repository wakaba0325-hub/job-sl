"""コレクタ run.py 共通の「止まらない」ための部品。

- Deadline: 1回の run の持ち時間。超えたら取得を打ち切り、そこまでの分で L2 を作って終了する
  (EventBridge は翌日また同時刻に起動するので、日をまたいで走り続けないようにする)。
- BlockGuard: 取得開始直後に全件が拒否される(WAF/IP単位のブロック)場合に、1万件以上を
  無駄に叩かず早期に打ち切る。拒否は Fargate の送信元IP単位で決まるため、少し待って
  タスクを起動し直す(retry_lambda)と別IPで成功することが多い。
- emit_summary: run の最後に JSON 1行を出す。CloudWatch Logs のメトリクスフィルタで
  成功/失敗/件数を拾い、媒体別ハートビートに使う。
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime

from job_sl import JST

# 一時的な失敗。retry_lambda が同じタスク定義を最大3回まで起動し直す。
EXIT_RETRYABLE = 75
EXIT_FAILED = 1


class Deadline:
    """`Deadline(hours)` を作った時刻からの持ち時間。hours=None/0 なら無期限。"""

    def __init__(self, hours: float | None):
        self._end = time.monotonic() + hours * 3600 if hours else None

    def expired(self) -> bool:
        return self._end is not None and time.monotonic() >= self._end


class BlockedByRemote(Exception):
    """取得開始直後に大半が拒否された(IP/WAFブロックの疑い)。"""


class BlockGuard:
    """最初の `window` 件の結果を見て、成功が `min_ok_ratio` 未満なら BlockedByRemote を投げる。
    window を超えたら以降は判定しない(途中からの散発的な失敗は通常のエラー扱い)。"""

    def __init__(self, window: int = 40, min_ok_ratio: float = 0.1):
        self.window = window
        self.min_ok_ratio = min_ok_ratio
        self.n = 0
        self.ok = 0

    def record(self, ok: bool) -> None:
        if self.n >= self.window:
            return
        self.n += 1
        self.ok += 1 if ok else 0
        if self.n == self.window and self.ok / self.n < self.min_ok_ratio:
            raise BlockedByRemote(
                f"先頭{self.window}件の成功が{self.ok}件。送信元IPがブロックされている疑い"
            )


def emit_summary(source: str, status: str, **counts) -> None:
    """status: ok / partial(時間切れ等で途中まで) / blocked / failed。
    CloudWatch Logs のメトリクスフィルタ用に JSON 1行で標準出力へ出す。"""
    print(
        json.dumps(
            {
                "event": "run_summary",
                "source": source,
                "status": status,
                "at": datetime.now(JST).isoformat(timespec="seconds"),
                **counts,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    sys.stderr.flush()
