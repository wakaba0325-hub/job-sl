"""ECSタスクの多重起動防止(排他起動ガード)。

前日分の処理が1日で完走しないまま翌日分のスケジュール起動(EventBridge)が走り、
同じfamilyのタスクが2つ同時にRUNNINGになって同一データに対して重複処理してしまう
事故を防ぐ。ECSタスク起動直後にentrypointの先頭で`ensure_exclusive`を呼ぶと、
同じtask_familyで既にRUNNING中の別タスクがある場合はexit code 0で即座に正常終了する
(EventBridge起動のECSタスクはリトライされないため、exit 0でスケジュールを消費して
静かに終わらせるのが正しい)。

自タスクのtaskArn/clusterはECSのタスクメタデータエンドポイント
(`ECS_CONTAINER_METADATA_URI_V4`環境変数、Fargateで自動注入)から取得する。
他タスクの有無は`ecs:ListTasks`(family+desiredStatus=RUNNINGで絞り込み)で確認する。
ECSタスクロールにこの権限が無いと排他チェックができないため、
ecsTaskExecutionRoleに`AllowEcsListTasksForSelfExclusionLock`
(emooove-etlクラスタに絞ったecs:ListTasks)をIAMインラインポリシーとして付与済み。
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request

import boto3
from botocore.config import Config as _BotoConfig
from botocore.exceptions import ClientError

REGION = "ap-northeast-1"

_ECS_CONFIG = _BotoConfig(
    region_name=REGION,
    connect_timeout=10,
    read_timeout=30,
    retries={"max_attempts": 3, "mode": "standard"},
)


def _self_task_metadata() -> dict | None:
    """ECS Container Metadata (v4) の `/task` エンドポイントから自タスク情報を取得。
    ECS外(ローカル実行など)では`ECS_CONTAINER_METADATA_URI_V4`が無いのでNoneを返す。"""
    uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
    if not uri:
        return None
    try:
        with urllib.request.urlopen(f"{uri}/task", timeout=10) as resp:
            return json.load(resp)
    except Exception as e:  # noqa: BLE001
        print(
            f"[ensure_exclusive] メタデータ取得失敗、排他チェックをスキップ: {e}",
            file=sys.stderr,
        )
        return None


def ensure_exclusive(task_family: str) -> None:
    """同じ`task_family`で既にRUNNING中の別タスクがあれば、このプロセスを
    exit code 0でスキップ終了する(前回実行が完了していないための重複起動防止)。

    - ECSタスクメタデータが取得できない場合(ローカル実行など)はチェックをスキップして
      何もせず戻る(=fail-open)。
    - `ecs:ListTasks`が権限エラー等で失敗した場合も、排他チェック自体で本来の処理を
      止めてしまわないようログを出してスキップする(=fail-open)。
    """
    meta = _self_task_metadata()
    if meta is None:
        return

    self_task_arn = meta.get("TaskARN")
    cluster_arn = meta.get("Cluster")
    if not self_task_arn or not cluster_arn:
        print(
            "[ensure_exclusive] taskArn/clusterが取得できず、排他チェックをスキップ",
            file=sys.stderr,
        )
        return

    try:
        ecs = boto3.client("ecs", config=_ECS_CONFIG)
        task_arns: list[str] = []
        paginator = ecs.get_paginator("list_tasks")
        for page in paginator.paginate(
            cluster=cluster_arn, family=task_family, desiredStatus="RUNNING"
        ):
            task_arns.extend(page.get("taskArns", []))
    except ClientError as e:
        print(
            f"[ensure_exclusive] ecs:ListTasks失敗、排他チェックをスキップして続行: {e}",
            file=sys.stderr,
        )
        return

    others = [a for a in task_arns if a != self_task_arn]
    if others:
        print(
            f"[ensure_exclusive] family={task_family} で既にRUNNING中の他タスクが"
            f"{len(others)}件あるため、今回の起動をスキップします(前回実行が未完了): "
            f"{others}",
            file=sys.stderr,
        )
        sys.exit(0)

    print(
        f"[ensure_exclusive] family={task_family} 排他チェックOK"
        "(他にRUNNING中タスク無し、通常起動を継続)",
        file=sys.stderr,
    )
