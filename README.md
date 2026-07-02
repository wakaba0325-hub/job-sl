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

L3(全ソース統合・company_master突合・新規掲載検知)は本パッケージのスコープ外(別リポで実装)。
