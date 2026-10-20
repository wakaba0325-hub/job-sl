#!/usr/bin/env bash
# 求人収集の「止まらない仕組み」。未適用。ユーザーが内容を確認してから実行する。
set -euo pipefail
R=ap-northeast-1; A=539691208375; CL=emooove-etl
TOPIC=arn:aws:sns:$R:$A:job-pipeline-alerts
ROLE=arn:aws:iam::$A:role/job-retry-lambda-role   # 事前に retry_lambda_policy.json を付けて作成

# 1. 通知先
aws sns create-topic --name job-pipeline-alerts --region $R
# aws sns subscribe --topic-arn $TOPIC --protocol email --notification-endpoint <通知先> --region $R

# 2. リトライLambda
(cd "$(dirname "$0")" && zip -q /tmp/retry_lambda.zip retry_lambda.py)
aws lambda create-function --function-name job-retry --runtime python3.12 --handler retry_lambda.handler \
  --role $ROLE --zip-file fileb:///tmp/retry_lambda.zip --timeout 60 \
  --environment "Variables={CLUSTER=$CL,SNS_TOPIC_ARN=$TOPIC,MAX_RETRY=3}" --region $R

# 3. ECSタスク停止イベント(exit!=0 / OOM / 起動失敗)をLambdaへ
aws events put-rule --name job-ecs-task-stopped-abnormal --region $R --event-pattern '{
  "source":["aws.ecs"],"detail-type":["ECS Task State Change"],
  "detail":{"clusterArn":["arn:aws:ecs:'$R':'$A':cluster/'$CL'"],"lastStatus":["STOPPED"],
    "containers":{"exitCode":[{"anything-but":0}]}}}'
aws lambda add-permission --function-name job-retry --statement-id events --action lambda:InvokeFunction \
  --principal events.amazonaws.com --source-arn arn:aws:events:$R:$A:rule/job-ecs-task-stopped-abnormal --region $R
aws events put-targets --rule job-ecs-task-stopped-abnormal --region $R \
  --targets "Id=1,Arn=arn:aws:lambda:$R:$A:function:job-retry"

# 4. 起動失敗(RunTask自体の失敗)
aws cloudwatch put-metric-alarm --alarm-name job-eventbridge-failed-invocations --region $R \
  --namespace AWS/Events --metric-name FailedInvocations --statistic Sum --period 3600 --evaluation-periods 1 \
  --threshold 1 --comparison-operator GreaterThanOrEqualToThreshold --treat-missing-data notBreaching \
  --alarm-actions $TOPIC

# 5. run_summary を各ロググループからメトリクス化(例: mynavi-baito。全収集ロググループに繰り返す)
for LG in /ecs/mynavi-baito-job-collector /ecs/kaigojob-job-collector /ecs/mynavi-job-collector /ecs/geekly-job-collector; do
  aws logs put-metric-filter --log-group-name $LG --filter-name run-summary-ok --region $R \
    --filter-pattern '{ $.event = "run_summary" && $.status = "ok" }' \
    --metric-transformations metricName=RunOk,metricNamespace=JobCollector,metricValue=1,defaultValue=0
  aws cloudwatch put-metric-alarm --alarm-name "job-heartbeat-$(basename $LG)" --region $R \
    --namespace JobCollector --metric-name RunOk --statistic Sum --period 86400 --evaluation-periods 2 \
    --threshold 1 --comparison-operator LessThanThreshold --treat-missing-data breaching --alarm-actions $TOPIC
done

# 6. 鮮度チェック(毎日 JST 23:30 = UTC 14:30)。job-master-consolidate のイメージ/ロールを流用しコマンドだけ上書き
aws ecs register-task-definition --region $R --cli-input-json file://"$(dirname "$0")"/freshness_taskdef.json
aws events put-rule --name job-freshness-daily --schedule-expression 'cron(30 14 * * ? *)' --region $R
# ターゲット登録は freshness_taskdef.json の ARN 確定後に ecs-parameters 付きで追加する(既存ruleの定義に倣う)
