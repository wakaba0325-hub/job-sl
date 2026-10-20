"""ECS Task State Change(STOPPED)を受け、exit 75 のタスクを同じタスク定義で再起動する。

再試行は最大 MAX_RETRY 回(タグ retry_n で数える)。OOM など exit 75 以外は再実行せず通知のみ。
"""

import json
import os

import boto3

ecs = boto3.client("ecs")
sns = boto3.client("sns")

CLUSTER = os.environ.get("CLUSTER", "emooove-etl")
SUBNET = os.environ.get("SUBNET", "subnet-094a8e84bc166be1f")
SG = os.environ.get("SG", "sg-0fb1e6654ab1792e6")
MAX_RETRY = int(os.environ.get("MAX_RETRY", "3"))
TOPIC = os.environ.get("SNS_TOPIC_ARN", "")
EXIT_RETRYABLE = 75


def _notify(subject: str, body: dict):
    if TOPIC:
        sns.publish(
            TopicArn=TOPIC,
            Subject=subject[:100],
            Message=json.dumps(body, ensure_ascii=False),
        )


def handler(event, _context):
    d = event["detail"]
    if (
        d.get("lastStatus") != "STOPPED"
        or d.get("clusterArn", "").rsplit("/", 1)[-1] != CLUSTER
    ):
        return {"skipped": True}
    c = d["containers"][0]
    code = c.get("exitCode")
    td = d["taskDefinitionArn"]
    family = td.rsplit("/", 1)[-1].split(":")[0]
    info = {
        "family": family,
        "task": d["taskArn"],
        "exitCode": code,
        "reason": c.get("reason"),
        "stoppedReason": d.get("stoppedReason"),
    }
    if code == 0:
        return {"ok": True}
    if code != EXIT_RETRYABLE:
        _notify(f"[job] {family} 異常終了 exit={code}", info)
        return {"notified": True}

    tags = {
        t["key"]: t["value"]
        for t in ecs.list_tags_for_resource(resourceArn=d["taskArn"]).get("tags", [])
    }
    n = int(tags.get("retry_n", "0"))
    if n >= MAX_RETRY:
        _notify(f"[job] {family} リトライ上限({MAX_RETRY})", info)
        return {"gave_up": True}

    overrides = {}
    if d.get("overrides", {}).get("containerOverrides"):
        overrides = {"containerOverrides": d["overrides"]["containerOverrides"]}
    r = ecs.run_task(
        cluster=CLUSTER,
        taskDefinition=td,
        launchType="FARGATE",
        count=1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": [SUBNET],
                "securityGroups": [SG],
                "assignPublicIp": "ENABLED",
            }
        },
        overrides=overrides,
        startedBy="job-retry",
        tags=[{"key": "retry_n", "value": str(n + 1)}],
    )
    return {"retried": [t["taskArn"] for t in r["tasks"]], "failures": r["failures"]}
