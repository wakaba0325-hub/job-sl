# job-sl

採用媒体の求人情報スクレイピング(`<source>-job-collector` 群)の共通土台。`keyman-sl` と同じ設計パターン(L1追記・L2 job_urlキーdedup)を踏襲。

## 仕組み

- L1: `raw/job_sources/<source>/{YYYYMMDD}/<source>.csv` — 当日取得分の追記ログ
- L2: `master/job_sources/<source>/{YYYYMMDD}/<source>.csv` — job_urlキーでdedupした正規化マスタ(最新日=全件)

各媒体は schema.org `JobPosting` 相当のフィールド(`COMMON_SCHEMA`)に正規化して渡す。全ソース共通で `job_url` を一意キーとする。

## 使い方

```python
import job_sl as jsl

rows = [{"company_name": "...", "job_title": "...", "job_url": "...", "scraped_at": "...", ...}]
_, n_new = jsl.write_l1("green", rows)
_, l2_key = jsl.build_l2("green")
```

`write_l1`/`build_l2`は呼ぶたびにそのソースの全L1履歴をS3から再ダウンロード・再パースする
(job_urlの重複排除のためだけに全カラムを読む設計)。1回のプロセス内で何度もチェックポイント
保存する長時間巡回コレクタ(マイナビ等)では`L1Checkpointer`を使い、全履歴スキャンを
プロセス開始時の1回だけにすること:

```python
cp = jsl.L1Checkpointer("mynavi0")
for batch in ...:
    n_new = cp.flush(batch)  # 当日L1パーティションへ都度保存、履歴re-scanなし
jsl.build_l2("mynavi0")  # build_l2はコストが高いのでrun完了時に1回だけ呼ぶ
```

## 掲載終了検知と求人状態(job_state)

「今日サイトで確認できたか」を `seen_urls` から判定し、求人URLごとの状態を `master/job_state/{YYYYMMDD}/job_state.csv`
に持つ(`job_sl/closure.py`)。`job_master`(十数GB)を前回状態として読まない。

- `listing_status`: `active`(当日確認できた) / `closed`(完走した巡回で**連続2回**確認できず、または確認できない状態で
  `valid_through` が過去) / `unknown`(判定不能)。理由は `status_reason`
  (seen / not_seen / valid_through_expired / no_seen_source / no_seen_file / shard_incomplete /
  source_partial_unstable / safety_ratio / miss_pending / stale_unconfirmed / migrated)
- 1回確認できなかっただけでは閉じない(`miss_pending`)。townwork/rikunabi 等は1日欠けて翌日戻る求人が1〜4割あるため。
  valid_through が過去の求人は1回で閉じる(ただし確認できた行の5%超で valid_through が過去の媒体=mynavi/ambi/hupro 等は
  valid_through が更新されていないため判定に使わない)。`CLOSE_AFTER_MISSES` で変更
- `first_seen_date` / `last_seen_date` / `closed_detected_date` を持つ。`valid_through` は補助(確認できない行だけに使う)
- 保持窓: closed は検知日から365日、unknown は最後に確認/初出した日から365日。超えたら job_master/job_state から落とす
- 安全装置: 分母は前日の **active件数**(旧実装は閉鎖・凍結行を含む累積件数で、シャード系は恒常スキップだった)。
  当日確認件数が70%未満なら媒体全体を保留(active は3日間は維持し、それ以上確認できなければ unknown)
- シャード媒体: seen は完走時のみ書かれ、シャード間は互いに素。URLのシャード割当が日次で安定(前日と同じシャードに入る率98%以上)
  な媒体は、欠けたシャードに入っていたURLだけ保留し他は判定する。割当が不安定な媒体(geekly/jac_recruitment/levwell_kango 等)は
  1シャードでも欠ければ媒体全体を保留する
- 異常: L2ソースの欠落や状態の行数激減は何も書込まず exit 1。完走しなかった媒体が(`closure.EXPECTED_NO_SEEN` 以外に)あれば
  書込のうえ exit 2

### L1の更新(refresh)

`write_l1(source, rows, refresh=True)` / `L1Checkpointer(source, refresh=True)` は、新規URLに加えて最新L2と内容
(`FINGERPRINT_COLUMNS`)が変わった既知URLも追記する。変更が無い既知URLは追記しない(last_seenは seen_urls→job_state が持つので
L1を書き換えない)。再取得するURLの選定には `stale_urls(source, older_than_days, limit)` を使う。

### 初回移行・世代保持

- 初回のみ `python -m job_sl.migrate_state --date YYYYMMDD [--apply]` で既存 job_master から job_state を作る
  (`--apply` なしはローカル出力のみ)。移行後の最初の build_l3 は移行日より後の日に走らせる。job_state が無いと build_l3 --apply は書込を拒否する
- `build_l3 --prune apply` で job_master 7世代 / job_state 14世代 / L2 14世代を超えた古い世代を削除(既定は削除予定の表示のみ。
  根拠は `job_sl/retention.py`)

## 過去分のクリーンアップ(backfill・一回限り)

`python -m job_sl.backfill <audit|rebuild-l2|replay-state|prune>`(すべて既定 dry-run。L1の原本には書かない・消さない)。

- `rebuild-l2 --sources a,b [--apply]`: L1の最新日がL2より新しい(build_l2がOOM等で凍結した)ソースを、L1全履歴からstreamingで作り直して
  当日フォルダへ新規書込(既存キーは上書きしない、1回3ソースまで)。引数なしで凍結ソース一覧
- `replay-state --end YYYYMMDD [--apply]`: 過去の seen_urls を日付順に `closure.Judge` へ通し、`master/job_state/{end}/` を復元
  (`migrate_state` の代わり。first_seen は L2 の scraped_at の最小、終了検知日は最後に確認できた日の翌日、
  一度も確認できないまま閉じた行は `backfill_never_seen`)。必ず `rebuild-l2` の後に実行
- `prune [--mode delete|backup] [--max-objects N] [--protect YYYYMMDD,..] [--apply]`: job_master 7 / job_state 14 / L2 14 世代を超えた古い世代を
  delete marker(または `_backup/` へ退避)。versioning が Enabled でなければ中止。未来日付・空の最新世代のグループは触らない
- `audit`: 最新L2の異常行(URL不正・社名空・未来日付)・グループ内重複・未来日付フォルダ・凍結L2の全量スキャン(読取のみ)

build_l3 は異常行(`rowcheck.row_issue`)を除外し、同一URLの重複は scraped_at が新しい行(同値なら情報が多い行)を採用、
取得日から2年超先の `valid_through`(mynavi_pharma の掲載日+20年など媒体の仮値)は空にして出力する。

## L3: job_master (全ソース統合・company_master突合・新規掲載検知)

`python -m job_sl.build_l3 [--apply]` (実行は別リポ `job-master-consolidate` から)で:

- `master/job_sources/<source>/{最新}/<source>.csv` を全ソースunion
- `company_name` を `company_master` の `商号_nor` と同一規則(`job_sl/l3.py` の `core_key`。
  `company-norm` パッケージの vendor版で、法人格36種+異体字統一まで対応)で正規化して法人番号突合
- 同名複数がヒットする場合は以下を順に試して一意化:
  1. 処理区分(国税庁法人番号データ)が閉鎖・取消系の候補を除外
  2. なお複数残れば、求人の勤務地(`location`)から都道府県を抽出し本店所在都道府県が
     一致する候補に絞り込み
  3. 完全未マッチの場合のみ、注記("(旧名：〜)"等)・拠点表記("〜支店"等)を除去して再突合
  - 解決できなければ `is_ambiguous_company=1`(法人番号は空)のまま
  - 採用した解決方法は `match_method` 列に記録(`exact` / `closure_excluded` /
    `pref_disambiguated` / `note_stripped` / `branch_stripped` / `ambiguous` / `unmatched`)
- シャード間で重複するURLは1行に畳む(scraped_atが新しい行を採用)
- 前回 `job_state` と比較し `first_seen_date` / `is_new_today` を付与(上の状態列も出力)
- `master/job_master/{YYYYMMDD}/job_master.csv` へスナップショット型で書込(全件洗い替え)

2026-07-07時点の実データ(23万件)での検証結果: マッチ率 57.2%→72.4%(あいまい件数 -35%)。
詳細は `job_sl/l3.py` のdocstring参照。
