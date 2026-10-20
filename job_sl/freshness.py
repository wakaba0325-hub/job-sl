"""媒体(シャード)ごとの「最終成功日」監視。S3 の一覧だけを見る(本文は読まない)。

    python -m job_sl.freshness --max-age-days 2 --put-metrics --sns-topic arn:aws:sns:...

判定(L2 = master/job_sources/<source>/{YYYYMMDD}/<source>.csv の最新日):
  alert  最終日から max_age_days 日以上更新が無い / L2 が無い(L1だけある)
  warn   直近の L2 が前回の 70% 未満に縮んだ / 直近 static_days 日 L2 のサイズが変わらず
         L1 も更新されていない(件数不変=実質停止) / 直近14日に歯抜けが3日以上
alert が1件でもあれば exit 1 で終了する。ECS の定期タスクとして動かせば、
「異常終了(exit≠0)の ECS タスク停止イベント → SNS」の既存ルールでそのまま通知できる。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta

import job_sl as _core

_DATE = re.compile(r"^(?:raw|master)/job_sources/([^/]+)/(\d{8})/([^/]+)$")

# 運用上すでに止めている/対象外の媒体。必要なら --ignore で上書きする。
DEFAULT_IGNORE = ("rikunabi", "hataractive", "workport")


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def scan(s3, prefix: str) -> dict[str, list[tuple[str, int]]]:
    """source -> [(YYYYMMDD, size)](`<source>.csv` 本体のみ・日付昇順)。"""
    out: dict[str, list[tuple[str, int]]] = defaultdict(list)
    pg = s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=_core.BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            m = _DATE.match(o["Key"])
            if m and m.group(3) == f"{m.group(1)}.csv":
                out[m.group(1)].append((m.group(2), o["Size"]))
    for v in out.values():
        v.sort()
    return out


def evaluate(
    l1: dict,
    l2: dict,
    today: date,
    max_age_days: int = 2,
    static_days: int = 14,
    ignore: tuple[str, ...] = DEFAULT_IGNORE,
) -> list[dict]:
    rows = []
    for src in sorted(set(l1) | set(l2)):
        if src in ignore:
            continue
        r = {"source": src, "level": "ok", "reasons": []}
        a2 = l2.get(src, [])
        a1 = l1.get(src, [])
        r["l1_latest"] = a1[-1][0] if a1 else None
        r["l2_latest"] = a2[-1][0] if a2 else None
        if not a2:
            r["level"] = "alert"
            r["reasons"].append("L2が無い")
            r["l2_age_days"] = None
            rows.append(r)
            continue
        age = (today - _d(a2[-1][0])).days
        r["l2_age_days"] = age
        if age >= max_age_days:
            r["level"] = "alert"
            r["reasons"].append(f"L2が{age}日更新なし(最終{a2[-1][0]})")
        if len(a2) >= 2 and a2[-1][1] < a2[-2][1] * 0.7:
            r["level"] = r["level"] if r["level"] == "alert" else "warn"
            r["reasons"].append(f"L2が縮小({a2[-2][1]:,}→{a2[-1][1]:,}B)")
        recent = [x for x in a2 if (today - _d(x[0])).days <= static_days]
        l1_recent = [x for x in a1 if (today - _d(x[0])).days <= static_days]
        if (
            len(recent) >= static_days // 2
            and len({sz for _, sz in recent}) == 1
            and not any(sz > 0 for _, sz in l1_recent)
        ):
            r["level"] = r["level"] if r["level"] == "alert" else "warn"
            r["reasons"].append(f"直近{static_days}日L2のサイズ不変・L1更新なし")
        have = {x[0] for x in a2}
        last14 = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(14)]
        first = a2[0][0]
        gaps = [x for x in last14 if x >= first and x not in have]
        if len(gaps) >= 3 and r["level"] == "ok":
            r["level"] = "warn"
            r["reasons"].append(f"直近14日に歯抜け{len(gaps)}日")
        rows.append(r)
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age-days", type=int, default=2)
    ap.add_argument("--static-days", type=int, default=14)
    ap.add_argument("--ignore", default=",".join(DEFAULT_IGNORE))
    ap.add_argument("--put-metrics", action="store_true")
    ap.add_argument("--sns-topic", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    import boto3

    s3 = _core._s3()
    today = datetime.now(_core.JST).date()
    l1 = scan(s3, "raw/job_sources/")
    l2 = scan(s3, "master/job_sources/")
    rows = evaluate(
        l1,
        l2,
        today,
        args.max_age_days,
        args.static_days,
        tuple(x for x in args.ignore.split(",") if x),
    )
    bad = [r for r in rows if r["level"] != "ok"]
    if args.json:
        print(json.dumps(rows, ensure_ascii=False, indent=1))
    else:
        for r in bad:
            print(
                f"[{r['level']}] {r['source']}: L2最新={r['l2_latest']} L1最新={r['l1_latest']} "
                + " / ".join(r["reasons"])
            )
        print(
            f"[freshness] 対象{len(rows)}媒体 alert={sum(r['level'] == 'alert' for r in rows)} "
            f"warn={sum(r['level'] == 'warn' for r in rows)}"
        )

    if args.put_metrics:
        cw = boto3.client("cloudwatch", region_name=_core.REGION)
        data = [
            {
                "MetricName": "L2AgeDays",
                "Dimensions": [{"Name": "Source", "Value": r["source"]}],
                "Value": float(
                    r["l2_age_days"] if r["l2_age_days"] is not None else 999
                ),
                "Unit": "Count",
            }
            for r in rows
        ]
        for i in range(0, len(data), 1000):
            cw.put_metric_data(Namespace="JobPipeline", MetricData=data[i : i + 1000])

    alerts = [r for r in rows if r["level"] == "alert"]
    if alerts and args.sns_topic:
        body = "\n".join(f"{r['source']}: " + " / ".join(r["reasons"]) for r in alerts)
        boto3.client("sns", region_name=_core.REGION).publish(
            TopicArn=args.sns_topic,
            Subject=f"[job-pipeline] L2更新停止 {len(alerts)}媒体",
            Message=body[:250000],
        )
    return 1 if alerts else 0


if __name__ == "__main__":
    sys.exit(main())
