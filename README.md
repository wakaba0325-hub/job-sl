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
- `job_url` を前回 `job_master` と比較し `first_seen_date` / `is_new_today` を付与
- `master/job_master/{YYYYMMDD}/job_master.csv` へスナップショット型で書込(全件洗い替え)

2026-07-07時点の実データ(23万件)での検証結果: マッチ率 57.2%→72.4%(あいまい件数 -35%)。
詳細は `job_sl/l3.py` のdocstring参照。
