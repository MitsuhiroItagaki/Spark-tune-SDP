# spark-perf-dlt

**Spark Job Performance Analysis Pipeline using Delta Live Tables**

## Background

Spark UI provides detailed execution metrics, but interpreting them requires deep knowledge of Spark internals. For general users, identifying the root cause of a slow job — whether it is data skew, shuffle pressure, GC overhead, or resource contention — and deciding on the right remediation can take hours of manual investigation across multiple UI tabs.

This tool was built to eliminate that burden by automating the entire analysis workflow: from raw event log ingestion to bottleneck classification and actionable recommendations, presented in a single dashboard.

## What is this?

`spark-perf-dlt` is a Databricks tool that automatically analyzes Spark job performance by parsing event logs and publishing an interactive Lakeview dashboard — no Spark UI access required.

### Purpose

Spark performance problems (data skew, excessive shuffle, GC pressure, low Photon utilization, resource contention) are hard to diagnose manually from logs. This tool automates the full analysis pipeline:

1. **Ingest** — Reads raw Spark event logs from S3 or Unity Catalog Volumes using Delta Live Tables Autoloader
2. **Parse** — Extracts structured metrics per application, job, stage, task, executor, and SQL execution
3. **Analyze** — Classifies bottlenecks, detects skew and stragglers, scores CPU/Photon efficiency
4. **Visualize** — Publishes a 7-page Lakeview dashboard with actionable recommendations

### Key Capabilities

| Capability | Description |
|---|---|
| **Bottleneck detection** | Automatically classifies stages as DISK_SPILL / DATA_SKEW / HEAVY_SHUFFLE / HIGH_GC / STAGE_FAILURE |
| **Skew analysis** | Task duration and data volume skew metrics (p50/p95/p99, skew ratio, skew gap) |
| **Shuffle analysis** | Stage-level shuffle read/write and fetch-wait time breakdown |
| **Executor analysis** | Load distribution with straggler detection (Z-score + relative load ratio) |
| **CPU / GC analysis** | Per-job CPU efficiency, GC overhead, and scheduling delay |
| **Photon analysis** | Per-SQL Photon utilization rate, join type breakdown, non-Photon operator list |
| **Concurrency analysis** | Concurrent job count at submission time vs. CPU efficiency |
| **Live cluster support** | Processes `.inprogress` log files from running clusters |

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tables](#tables)
- [Bottleneck Detection](#bottleneck-detection)
- [Setup](#setup)
- [Pipeline Configuration](#pipeline-configuration)
- [Dashboard](#dashboard)
- [Workflow Automation](#workflow-automation)
- [Testing](#testing)
- [File Structure](#file-structure)
- [日本語ドキュメント](#日本語ドキュメント)

---

## Overview

This pipeline reads Spark event log files directly from cloud storage, eliminating the dependency on the Spark UI or driver-proxy-api. It works for both **running clusters** (`.inprogress` files) and **terminated clusters** (completed log files).

```
Event Logs (S3 / UC Volume)
        │
        ▼
┌───────────────┐
│    Bronze     │  Raw JSON lines (1 table)
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Silver     │  Parsed by event type (7 tables)
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     Gold      │  Aggregated metrics + bottleneck analysis (7 tables)
└───────────────┘
        │
        ▼
┌───────────────┐
│   Dashboard   │  Lakeview (7 pages)
└───────────────┘
```

---

## Architecture

### Bronze (1 table)

| Table | Description |
|---|---|
| `bronze_raw_events` | Raw text lines ingested from event log files. One row per event. Supports recursive file lookup including `.inprogress` files. |

### Silver (7 tables)

| Table | Source Event | Description |
|---|---|---|
| `silver_application_events` | `SparkListenerApplicationStart/End` | Application start/end timing and user info |
| `silver_job_events` | `SparkListenerJobStart/End` | Job submission/completion with stage ID list |
| `silver_stage_events` | `SparkListenerStageCompleted` | Stage metrics extracted from Accumulables (I/O, Shuffle, GC, Spill) |
| `silver_task_events` | `SparkListenerTaskEnd` | Per-task metrics: duration, GC time, CPU time (ns), executor run time |
| `silver_executor_events` | `SparkListenerExecutorAdded/Removed` | Executor lifecycle and core allocation |
| `silver_spark_config` | `SparkListenerEnvironmentUpdate` | Performance-relevant Spark configuration key-value pairs |
| `silver_sql_executions` | `SparkListenerSQLExecutionStart/End` | SQL execution events joined with physical plan description |

### Gold (7 tables)

| Table | Description |
|---|---|
| `gold_application_summary` | Application-level KPIs (duration, job success rate, total shuffle/spill GB, GC overhead) |
| `gold_job_performance` | Job-level duration and status with stage ID list |
| `gold_stage_performance` | Stage-level analysis with task distribution (p50/p75/p95/p99), per-task shuffle stats, skew metrics, scheduling delay, bottleneck classification, and recommendations |
| `gold_executor_analysis` | Executor load distribution with straggler detection using Z-score and relative load ratio |
| `gold_bottleneck_report` | Prioritized list of all bottleneck stages with recommended actions |
| `gold_job_concurrency` | Per-job concurrent execution count at start time, plus CPU metrics (efficiency, total CPU/GC time) from task events |
| `gold_sql_photon_analysis` | Per-SQL-execution Photon utilization, operator counts, join type breakdown (BHJ / SMJ / PhotonBHJ), and non-Photon operator list |
| `gold_narrative_summary` | LLM-generated analysis history — summary text and TOP3 improvement recommendations per run (model name, token counts, timestamp) |

---

## Tables

### `gold_stage_performance` — Key Columns

| Column | Description |
|---|---|
| `duration_ms` | Total stage duration in milliseconds |
| `gc_overhead_pct` | JVM GC time as % of executor run time |
| `cpu_efficiency_pct` | Executor CPU time / Executor run time × 100 |
| `task_p50_ms` / `task_p95_ms` / `task_p99_ms` | Task duration percentiles |
| `task_skew_ratio` | `task_max_ms / task_p50_ms` — ratio > 5 indicates skew |
| `time_skew_gap_ms` | `task_max_ms - task_p50_ms` — absolute time gap due to skew |
| `shuffle_read_mb` / `shuffle_write_mb` | Stage-level shuffle I/O in MB |
| `task_shuffle_min_mb` / `task_shuffle_p50_mb` / `task_shuffle_max_mb` | Per-task shuffle read distribution |
| `shuffle_skew_ratio` | `task_shuffle_max / task_shuffle_p50` — data volume skew indicator |
| `data_skew_gap_mb` | `task_shuffle_max - task_shuffle_min` — data volume gap between tasks |
| `disk_spill_mb` / `memory_spill_mb` | Spill size in MB |
| `scheduling_delay_ms` | Time from stage submission to first task launch — executor wait time |
| `shuffle_fetch_wait_ms` | Time tasks spent waiting to fetch shuffle data |
| `bottleneck_type` | Classified bottleneck type (see below) |
| `recommendation` | Suggested tuning action |

### `gold_job_concurrency` — Key Columns

| Column | Description |
|---|---|
| `concurrent_jobs_at_start` | Number of other jobs running when this job was submitted |
| `job_cpu_efficiency_pct` | `sum(executor_cpu_time_ns) / sum(executor_run_time_ms)` × 100 |
| `total_cpu_time_sec` | Total CPU wall time consumed across all tasks |
| `total_exec_run_time_sec` | Total executor run time across all tasks |
| `avg_task_cpu_time_ms` | Average CPU time per task |
| `total_gc_time_sec` | Total GC time across all tasks |

### `gold_sql_photon_analysis` — Key Columns

| Column | Description |
|---|---|
| `photon_pct` | % of operators using Photon |
| `is_photon` | `true` if any Photon operator present |
| `bhj_count` | Number of BroadcastHashJoin operators |
| `photon_bhj_count` | Number of PhotonBroadcastHashJoin operators |
| `smj_count` | Number of SortMergeJoin operators |
| `total_join_count` | Total join operator count |
| `non_photon_op_list` | Comma-separated list of distinct non-Photon operators |

### `gold_executor_analysis` — Key Columns

| Column | Description |
|---|---|
| `load_vs_avg` | Executor's total task time / app average — ratio > 1.5 = straggler |
| `z_score` | Z-score of task duration distribution |
| `is_straggler` | `true` if `load_vs_avg > 1.5` |
| `is_underutilized` | `true` if `load_vs_avg < 0.5` |
| `avg_gc_pct` | Average GC overhead across all tasks on this executor |

---

## Bottleneck Detection

The pipeline classifies each stage into one of the following bottleneck types:

| Type | Condition | Severity | Recommendation |
|---|---|---|---|
| `STAGE_FAILURE` | Stage failed | HIGH | Check `failure_reason`; increase executor memory for OOM. Note: AQE-driven cancellations are normal and not true failures. |
| `DISK_SPILL` | `disk_bytes_spilled > 0` | HIGH | Increase executor memory; enable AQE |
| `HIGH_GC` | GC overhead > 20% | MEDIUM | Review UDFs; reduce object creation; tune G1GC |
| `DATA_SKEW` | `task_skew_ratio > 5` | MEDIUM | Enable AQE skew join; salt join keys |
| `HEAVY_SHUFFLE` | Shuffle read > 10 GB | LOW | Use broadcast joins; tune shuffle partitions |
| `MEMORY_SPILL` | `memory_bytes_spilled > 0` | LOW | Increase `spark.memory.fraction`; repartition |
| `MODERATE_GC` | GC overhead > 10% | LOW | Monitor GC trends; consider more executor memory |
| `OK` | None of the above | NONE | No action needed |

---

## Data Safety

Spark event logs record **execution metadata and performance metrics only**. The actual data being processed (row values, cell contents, query results) is never written to the event log.

### What is included

| Category | Contents |
|---|---|
| Application metadata | App name, start/end time, submitting user name |
| Job / Stage / Task metrics | Duration, shuffle read/write bytes, GC time, CPU time, spill size |
| Executor information | Host, core count, lifecycle events |
| Spark configuration | Key-value pairs from `SparkListenerEnvironmentUpdate` |
| SQL physical plan | Operator names, join types, **table and column names** |

### What is NOT included

- ✅ Actual data records (row values, cell contents)
- ✅ Query result data
- ✅ Contents of files or tables being processed

### Caveats

- **Schema metadata:** SQL physical plans include table names and column names. If this information is sensitive, restrict access to the event log storage.
- **Spark configuration:** Spark config values are logged. Avoid storing credentials or secrets directly in Spark config properties. Use [Databricks Secrets](https://docs.databricks.com/en/security/secrets/index.html) instead.

---

## Setup

### Prerequisites

- Databricks workspace with Unity Catalog enabled (for UC Volume) or S3 access
- Spark event log delivery configured on the target job cluster
- Git integration enabled in Databricks
- SQL warehouse (for dashboard queries)

### 1. Clone to Databricks Repos

```
Databricks UI → Workspace → Repos → Add repo
  URL: https://github.com/mitsuhiro-itagaki_data/spark-perf-dlt
```

### 2. Create the DLT Pipeline

```
Workflows → Delta Live Tables → Create Pipeline
  Source code  : /Repos/<your-email>/spark-perf-dlt/pipeline.py
  Pipeline mode: Development (for testing) / Production (for scheduled runs)
```

### 3. Configure Event Log Delivery on the Job Cluster

Spark event logs must be written to a location accessible from Databricks (UC Volume or S3). There are two configuration methods.

> **Recommendation:** **UC Volume** is recommended as it allows you to browse and verify logs entirely within the Databricks UI without any additional access configuration.

#### Method A — Cluster Log Delivery (Databricks UI, recommended)

This is the simplest approach. Databricks automatically writes event logs to the specified path without any Spark config changes. Make sure the destination directory exists before starting the cluster.

> **The values below are examples. Replace the paths with those that match your environment.**

```
Cluster settings → Advanced options → Logging
  Destination : S3 / DBFS / UC Volume
  Log path    : s3://your-bucket/cluster-logs                       ← for S3
              : /Volumes/catalog/schema/volume/cluster-logs          ← for UC Volume
```

Logs are automatically stored at the path below. A directory named after the `<cluster_id>` is automatically created under the specified log path:
```
<log_path>/<cluster_id>/eventlog/
```

#### Method B — Spark Configuration

Add the following to the cluster's Spark config.

> **The values below are examples. Replace `spark.eventLog.dir` with the path that matches your environment.**

```
spark.eventLog.enabled  true
spark.eventLog.dir      s3://your-bucket/cluster-logs               # for S3
# or for UC Volume:
spark.eventLog.dir      /Volumes/catalog/schema/volume/cluster-logs
```

#### Finding the Cluster ID

The cluster ID is shown in the Databricks UI cluster detail page URL:

```
https://<workspace>/compute/clusters/<cluster_id>
# e.g.  0227-030422-j1llcyz8
```

It is also visible on the cluster detail page under **Tags** or **Configuration**.

#### Expected Log Directory Structure

After the cluster runs, logs will be available at:

```
<log_path>/
└── <cluster_id>/
    └── eventlog/
        ├── eventlog              ← completed cluster
        └── eventlog.inprogress  ← running cluster (also supported)
```

#### Verification

Check from a Databricks notebook:

```python
# UC Volume
dbutils.fs.ls("/Volumes/catalog/schema/volume/cluster-logs/<cluster_id>/eventlog/")

# S3
dbutils.fs.ls("s3://your-bucket/cluster-logs/<cluster_id>/eventlog/")
```

> **If using UC Volume, you can also browse the log files directly in the Databricks UI: Catalog → (your volume) → navigate to the log path.**

#### Analyzing Logs in a Different Environment

To analyze logs in a different Databricks workspace, download the subfolder under `<cluster_id>`. Using the UC Volume download feature in the Databricks UI, the entire folder is automatically compressed into a single ZIP file for download.

```
Databricks UI → Catalog → (target volume) → select <cluster_id> folder → Download
```

> **Note:** If the download size exceeds several GB, the UI download may become unstable. In that case, consider using the Databricks CLI or other file transfer methods instead.

> **Note:** The pipeline uses `recursiveFileLookup=true`, so `.inprogress` files from running clusters are automatically included. This allows you to analyze progress without waiting for the cluster to terminate. However, results from an in-progress analysis are limited to data available at that point in time — re-run the pipeline after the cluster finishes to get complete results.

---

## Pipeline Configuration

Set the following in DLT pipeline **Configuration** tab:

| Parameter | Required | Example | Description |
|---|---|---|---|
| `pipeline.log_root` | ✅ | `s3://bucket/cluster-logs` | Root path containing cluster log directories |
| `pipeline.cluster_id` | ✅ | `0123-456789-xxxxx` | Target cluster ID. Resolved path: `{log_root}/{cluster_id}/eventlog/` |

**Example (UC Volume):**
```json
{
  "pipeline.log_root":   "/Volumes/main/default/my_volume/cluster-logs",
  "pipeline.cluster_id": "0123-456789-xxxxx"
}
```

**Example (S3):**
```json
{
  "pipeline.log_root":   "s3://your-bucket/cluster-logs",
  "pipeline.cluster_id": "0123-456789-xxxxx"
}
```

> **Note:** The pipeline uses `recursiveFileLookup=true`, so it automatically picks up `.inprogress` files from running clusters.

---

## Workflow Automation

A Databricks Workflow orchestrates the full pipeline end-to-end:

```
[Task 1] dlt_pipeline
    DLT pipeline runs — Bronze / Silver / Gold tables updated
    ↓ (on success)
[Task 2] generate_summary
    Gold tables queried → LLM called → dashboard text updated
```

### Running the Workflow

```
Databricks UI → Workflows → "Spark Performance Analysis — Full Pipeline" → Run now
```

Or trigger via CLI:

```bash
unset DATABRICKS_TOKEN
databricks jobs run-now --job-id 1101932160868556
```

### LLM Model Selection

Edit the `MODEL_ENDPOINT` parameter in the `generate_summary_notebook` CONFIGURATION cell:

| Model | Endpoint name |
|---|---|
| Claude Opus 4.6 (recommended) | `databricks-claude-opus-4-6` |
| Claude Sonnet 4.6 | `databricks-claude-sonnet-4-6` |
| GPT-5.2 | `databricks-gpt-5-2` |
| Llama 3.3 70B | `databricks-meta-llama-3-3-70b-instruct` |

Generation history is saved to `gold_narrative_summary` Delta table for auditing.

---

## Dashboard

`create_dashboard_notebook` creates and publishes a Lakeview dashboard with 7 analysis pages.

### Pages

| Page | Content |
|---|---|
| **概要** (Overview) | Summary text with bottleneck analysis + TOP 3 improvement jobs, KPI counters, job duration bar chart, bottleneck report table, bottleneck type distribution |
| **ステージ分析** (Stage Analysis) | Scatter: duration vs skew ratio; bar: stage duration by bottleneck type; stage detail table |
| **Executor 分析** (Executor Analysis) | Load distribution bar chart; executor detail table |
| **スキュー分析** (Skew Analysis) | Time skew gap bar; data skew gap bar; scatter: data skew vs time skew; skew detail table |
| **SQL / Photon 分析** | Photon rate counters (avg / top 5% / top 10%); operator & join type counters; SQL detail table with non-Photon operator list |
| **Shuffle 分析** (Shuffle Analysis) | Shuffle read/write bars; fetch wait bar; executor shuffle bar; shuffle detail table |
| **並列実行分析** (Parallel Execution Analysis) | Concurrency & CPU efficiency counters; scatter: concurrent jobs vs CPU efficiency/duration; CPU efficiency & concurrency bars; scheduling delay bar; job detail table |

### Creating / Updating the Dashboard

Open `create_dashboard_notebook` in the Databricks workspace and run it directly. Edit the CONFIGURATION cell at the top as needed.

| Parameter | Description |
|---|---|
| `SCHEMA` | Unity Catalog schema containing the Gold tables (e.g. `main.base2`) |
| `WAREHOUSE_ID` | SQL warehouse ID for dashboard queries |
| `PARENT_PATH` | Workspace path where the dashboard file is saved |
| `DASH_NAME` | Dashboard display name |
| `EXISTING_DASHBOARD_ID` | Leave `None` to auto-detect by name; set an explicit ID if needed |

> **Note:** The script detects an existing dashboard by workspace path (`PARENT_PATH/DASH_NAME.lvdash.json`) and updates it in-place via PATCH. The dashboard URL never changes across updates.

---

## Testing

### Step 1: Generate sample event log data

Open `generate_test_data.py` as a Databricks notebook, update `LOG_ROOT` and `CLUSTER_ID`, then run it.

The script generates an event log covering all bottleneck scenarios:

| Stage | Scenario |
|---|---|
| Stage 0 | Normal (OK) |
| Stage 1 | Data Skew — one task 50× slower than median |
| Stage 2 | Disk Spill — 10 GB disk spill |
| Stage 3 | High GC — 28% GC overhead |
| Stage 4 | Heavy Shuffle — 15 GB shuffle read |
| Stage 5 | Stage Failure |

### Step 2: Run the pipeline in Development mode

```json
{
  "pipeline.log_root":   "/Volumes/main/default/my_volume/cluster-logs",
  "pipeline.cluster_id": "0123-456789-test"
}
```

### Step 3: Validate results

```sql
-- All bottleneck types should appear
SELECT bottleneck_type, severity, stage_name, recommendation
FROM gold_bottleneck_report
ORDER BY severity;

-- Stage 1 should have the highest skew ratio
SELECT stage_id, stage_name, task_skew_ratio, task_p50_ms, task_max_ms, time_skew_gap_ms
FROM gold_stage_performance
ORDER BY task_skew_ratio DESC NULLS LAST;

-- Photon utilization per SQL execution
SELECT description_short, duration_sec, photon_pct, non_photon_op_list
FROM gold_sql_photon_analysis
ORDER BY duration_sec DESC;

-- Jobs with high concurrency and low CPU efficiency
SELECT job_id, concurrent_jobs_at_start, job_cpu_efficiency_pct, duration_sec
FROM gold_job_concurrency
ORDER BY concurrent_jobs_at_start DESC;
```

---

## File Structure

```
spark-perf-dlt/
├── pipeline.py                      # DLT pipeline (Bronze / Silver / Gold)
├── create_dashboard_notebook.py     # Lakeview dashboard creation notebook (Databricks workspace)
├── generate_summary_notebook.py     # LLM-based AI summary generation notebook (auto-triggered by Workflow)
├── generate_test_data.py            # Sample event log generator for testing
└── README.md                        # This file
```

---

---

# 日本語ドキュメント

## 背景

Spark UI は詳細な実行メトリクスを提供しますが、その内容を正しく読み解くには Spark 内部の深い知識が必要です。一般ユーザーが「ジョブが遅い原因はデータスキューなのか、シャッフルなのか、GC なのか、リソース競合なのか」を特定し、適切な対応策を判断するには、複数の UI 画面を行き来しながら数時間の手動調査が必要になることがあります。

このツールは、そのような調査工数を削減するために開発しました。イベントログの取り込みからボトルネックの自動分類・改善提案の提示まで、分析ワークフロー全体を自動化し、結果を 1 つのダッシュボードに集約します。

## このツールについて

`spark-perf-dlt` は、Spark イベントログを自動解析してパフォーマンスのボトルネックを検出し、インタラクティブな Lakeview ダッシュボードとして可視化する Databricks ツールです。Spark UI へのアクセスは不要です。

### 目的

データスキュー・過剰なシャッフル・GC 負荷・Photon 利用率の低下・リソース競合といった Spark のパフォーマンス問題は、ログを手動で調べるには限界があります。このツールは解析パイプライン全体を自動化します：

1. **取り込み** — Delta Live Tables の Autoloader で S3 / UC Volume から Spark イベントログを自動読み込み
2. **パース** — アプリケーション・ジョブ・ステージ・タスク・Executor・SQL 実行ごとに構造化メトリクスを抽出
3. **分析** — ボトルネックを自動分類し、スキュー・ストラグラー・CPU/Photon 効率をスコアリング
4. **可視化** — 改善提案付きの 7 ページ Lakeview ダッシュボードを自動公開

### 主な機能

| 機能 | 内容 |
|---|---|
| **ボトルネック自動検出** | ステージを DISK_SPILL / DATA_SKEW / HEAVY_SHUFFLE / HIGH_GC / STAGE_FAILURE に自動分類 |
| **スキュー分析** | タスク実行時間・データ量スキューの指標（p50/p95/p99・スキュー比率・スキューギャップ） |
| **Shuffle 分析** | ステージ単位の Read/Write 量と Fetch Wait 時間の内訳 |
| **Executor 分析** | 負荷分散とストラグラー検出（Zスコア＋相対負荷比） |
| **CPU / GC 分析** | ジョブ単位の CPU 効率・GC オーバーヘッド・スケジューリングディレイ |
| **Photon 分析** | SQL実行ごとの Photon 利用率・Join 種別内訳・非Photonオペレータ一覧 |
| **並列実行分析** | ジョブ投入時の同時実行数と CPU 効率の相関 |
| **実行中クラスタ対応** | `.inprogress` ログファイルからリアルタイム解析が可能 |

---

## アーキテクチャ

### Bronze (1テーブル)

| テーブル | 説明 |
|---|---|
| `bronze_raw_events` | イベントログファイルから取り込んだ生テキスト行。1行1イベント。`.inprogress` ファイルも含む再帰探索対応。 |

### Silver (7テーブル)

| テーブル | 元イベント | 説明 |
|---|---|---|
| `silver_application_events` | ApplicationStart/End | アプリケーション開始・終了タイミングとユーザー情報 |
| `silver_job_events` | JobStart/End | ジョブの投入・完了とステージID一覧 |
| `silver_stage_events` | StageCompleted | Accumulables から I/O・Shuffle・GC・Spill メトリクスを抽出 |
| `silver_task_events` | TaskEnd | タスク単位の実行時間・GC時間・CPU時間(ns)・Executor実行時間 |
| `silver_executor_events` | ExecutorAdded/Removed | Executor のライフサイクルとコア数 |
| `silver_spark_config` | EnvironmentUpdate | パフォーマンス関連の Spark 設定値 |
| `silver_sql_executions` | SQLExecutionStart/End | SQL実行イベントと物理プラン説明文の結合 |

### Gold (7テーブル)

| テーブル | 説明 |
|---|---|
| `gold_application_summary` | アプリ全体 KPI（実行時間・ジョブ成功率・Shuffle/Spill GB・GCオーバーヘッド） |
| `gold_job_performance` | ジョブ単位の実行時間・成否・ステージID一覧 |
| `gold_stage_performance` | ステージ分析（タスク分布 p50/p75/p95/p99・タスク単位Shuffleデータ分布・スキュー指標・スケジューリングディレイ・ボトルネック分類・推奨対応付き） |
| `gold_executor_analysis` | Executor 負荷分散（Zスコアと相対負荷比によるストラグラー検出） |
| `gold_bottleneck_report` | 全ステージのボトルネックを重要度順にまとめた推奨アクション一覧 |
| `gold_job_concurrency` | ジョブ開始時の同時実行数と CPU 指標（効率・合計CPU/GC時間） |
| `gold_sql_photon_analysis` | SQL実行単位の Photon 利用率・オペレータ数・Join 種別内訳・非Photonオペレータ一覧 |
| `gold_narrative_summary` | LLM による分析テキスト生成履歴（サマリー・改善TOP3・モデル名・トークン数・生成日時） |

---

## ボトルネック検出

各ステージを以下の7種類に自動分類します：

| タイプ | 判定条件 | 重要度 | 推奨対応 |
|---|---|---|---|
| `STAGE_FAILURE` | ステージ失敗 | HIGH | `failure_reason` を確認。OOM の場合は executor memory を増加。※AQEによる自動キャンセルは正常動作のため除外 |
| `DISK_SPILL` | disk spill > 0 | HIGH | executor memory を増加。AQE を有効化 |
| `HIGH_GC` | GC オーバーヘッド > 20% | MEDIUM | UDF・collect の見直し。G1GC チューニング |
| `DATA_SKEW` | タスクスキュー比率 > 5 | MEDIUM | AQE スキュー結合を有効化またはキーのサルティング |
| `HEAVY_SHUFFLE` | Shuffle Read > 10 GB | LOW | Broadcast Join の検討・shuffle partitions 調整 |
| `MEMORY_SPILL` | memory spill > 0 | LOW | `spark.memory.fraction` 増加・repartition |
| `MODERATE_GC` | GC オーバーヘッド > 10% | LOW | GC 傾向の監視・executor memory の増加を検討 |

---

## データの安全性

Spark イベントログに記録されるのは**実行メタデータとパフォーマンス指標のみ**です。処理対象の実データ（行の値・セルの内容・クエリ結果）はイベントログに書き出されません。

### 含まれる情報

| カテゴリ | 内容 |
|---|---|
| アプリケーション情報 | アプリ名・開始/終了時刻・実行ユーザー名 |
| ジョブ / ステージ / タスク指標 | 実行時間・Shuffle Read/Write バイト数・GC 時間・CPU 時間・Spill サイズ |
| Executor 情報 | ホスト名・コア数・ライフサイクルイベント |
| Spark 設定値 | `SparkListenerEnvironmentUpdate` に含まれるキーと値 |
| SQL 物理プラン | オペレータ名・Join 種別・**テーブル名・カラム名** |

### 含まれない情報

- ✅ 実際のデータレコード（行の値・セルの内容）
- ✅ クエリの結果データ
- ✅ 処理対象のファイルやテーブルの内容

### 注意事項

- **スキーマ情報:** SQL 物理プランにテーブル名やカラム名が含まれます。これらの情報が機密に当たる場合は、イベントログの保存先へのアクセス権限を適切に制限してください。
- **Spark 設定値:** Spark の設定値がログに記録されます。認証情報やシークレットを Spark config に直接記述しないでください。[Databricks Secrets](https://docs.databricks.com/en/security/secrets/index.html) の使用を推奨します。

---

## セットアップ

### 1. Databricks Repos にクローン

```
Databricks UI → Workspace → Repos → Add repo
  URL: https://github.com/mitsuhiro-itagaki_data/spark-perf-dlt
```

### 2. DLT パイプライン作成

```
Workflows → Delta Live Tables → Create Pipeline
  Source code  : /Repos/<your-email>/spark-perf-dlt/pipeline.py
  Pipeline mode: Development（テスト時）/ Production（定期実行時）
```

### 3. ジョブクラスタのイベントログ出力設定

Spark イベントログを Databricks からアクセス可能な場所（UC Volume または S3）に書き出す設定が必要です。2つの方法があります。

> **推奨:** Databricks UI からログの確認・参照などの操作が完結できる **UC Volume** の使用を推奨します。

#### 方法 A — クラスタのログ配信設定（Databricks UI、推奨）

最も簡単な方法です。Spark 設定を変更せずに、Databricks が自動的にイベントログを指定パスに書き出します。指定するパスは事前にディレクトリを作成しておいてください。

> **以下は設定例です。パスはご利用の環境に合わせて変更してください。**

```
クラスタ設定 → Advanced options → Logging
  Destination : S3 / DBFS / UC Volume
  Log path    : s3://your-bucket/cluster-logs          ← S3 の場合
              : /Volumes/catalog/schema/volume/cluster-logs  ← UC Volume の場合
```

ログは以下のパスに自動的に格納されます。指定したパス配下に `<cluster_id>` と同じ名称のディレクトリが自動的に作成されます：

```
<log_path>/<cluster_id>/eventlog/
```

#### 方法 B — Spark 設定

クラスタの Spark 設定に以下を追加します。

> **以下は設定例です。`spark.eventLog.dir` のパスはご利用の環境に合わせて変更してください。**

```
spark.eventLog.enabled  true
spark.eventLog.dir      s3://your-bucket/cluster-logs          # S3 の場合
# UC Volume の場合:
spark.eventLog.dir      /Volumes/catalog/schema/volume/cluster-logs
```

#### クラスタ ID の確認方法

クラスタ ID は Databricks UI のクラスタ詳細ページの URL に含まれています：

```
https://<workspace>/compute/clusters/<cluster_id>
# 例: 0227-030422-j1llcyz8
```

クラスタ詳細ページの **Tags** や **Configuration** タブでも確認できます。

#### ログのディレクトリ構造

クラスタが実行されると、以下のようにログが作成されます：

```
<log_path>/
└── <cluster_id>/
    └── eventlog/
        ├── eventlog              ← 終了済みクラスタのログ
        └── eventlog.inprogress  ← 実行中クラスタのログ（このツールも対応済み）
```

#### ログの存在確認

Databricks ノートブックから確認できます：

```python
# UC Volume の場合
dbutils.fs.ls("/Volumes/catalog/schema/volume/cluster-logs/<cluster_id>/eventlog/")

# S3 の場合
dbutils.fs.ls("s3://your-bucket/cluster-logs/<cluster_id>/eventlog/")
```

> **UC Volume を使用している場合は、Databricks UI の「Catalog」→ ボリューム → 該当パスを開くことでもログファイルの存在を確認できます。**

#### 別環境でのログ分析

ログを別の Databricks ワークスペースで分析する場合は、`<cluster_id>` 以下のサブフォルダをダウンロードすることで対応できます。UC Volume のダウンロード機能を使用すると、フォルダ全体が自動的に 1 つの ZIP ファイルに圧縮されてダウンロードできます。

```
Databricks UI → Catalog → (対象ボリューム) → <cluster_id> フォルダを選択 → ダウンロード
```

> **Note:** ダウンロードファイルサイズが数 GB を超える場合は、UI からのダウンロードが不安定になることがあります。その場合は Databricks CLI や他の転送手段の利用を検討してください。

> **Note:** パイプラインは `recursiveFileLookup=true` を使用しているため、実行中クラスタの `.inprogress` ファイルも自動的に読み込まれます。これにより、クラスタの終了を待たずに途中経過を分析できます。ただし、実行中の分析結果はその時点までのデータに限られるため、完全な分析結果を得るにはクラスタ終了後にパイプラインを再実行してください。

---

## パイプライン設定パラメータ

DLT パイプラインの **Configuration** タブで設定します：

| パラメータ | 必須 | 例 | 説明 |
|---|---|---|---|
| `pipeline.log_root` | ✅ | `s3://bucket/cluster-logs` | クラスタログが格納されたルートパス |
| `pipeline.cluster_id` | ✅ | `0123-456789-xxxxx` | 対象クラスタの ID。読み込みパス: `{log_root}/{cluster_id}/eventlog/` |

**設定例（UC Volume）：**
```json
{
  "pipeline.log_root":   "/Volumes/main/default/my_volume/cluster-logs",
  "pipeline.cluster_id": "0123-456789-xxxxx"
}
```

**設定例（S3）：**
```json
{
  "pipeline.log_root":   "s3://your-bucket/cluster-logs",
  "pipeline.cluster_id": "0123-456789-xxxxx"
}
```

---

## ワークフロー自動化

Databricks Workflow がパイプライン全体をエンドツーエンドで自動実行します：

```
[タスク 1] dlt_pipeline
    DLT パイプライン実行 — Bronze / Silver / Gold テーブル更新
    ↓ （成功時）
[タスク 2] generate_summary
    Gold テーブルをクエリ → LLM 呼び出し → ダッシュボードのテキストを自動更新
```

### Workflow の実行

```
Databricks UI → Workflows → "Spark Performance Analysis — Full Pipeline" → Run now
```

CLI から実行する場合：

```bash
unset DATABRICKS_TOKEN
databricks jobs run-now --job-id 1101932160868556
```

### LLM モデルの選択

`generate_summary_notebook` の CONFIGURATION セルの `MODEL_ENDPOINT` を変更するだけで切り替え可能です：

| モデル | エンドポイント名 |
|---|---|
| Claude Opus 4.6（推奨・最高精度） | `databricks-claude-opus-4-6` |
| Claude Sonnet 4.6（精度とコストのバランス） | `databricks-claude-sonnet-4-6` |
| GPT-5.2 | `databricks-gpt-5-2` |
| Llama 3.3 70B（OSS） | `databricks-meta-llama-3-3-70b-instruct` |

生成履歴は `gold_narrative_summary` Delta テーブルに追記保存されます（監査・比較用）。

---

## ダッシュボード

`create_dashboard_notebook` を実行すると Lakeview ダッシュボード（7ページ）が作成・公開されます。

| ページ | 内容 |
|---|---|
| **概要** | ボトルネック分析サマリーテキスト・TOP3改善ジョブ分析・KPIカウンター・ジョブ実行時間棒グラフ・ボトルネック種別分布 |
| **ステージ分析** | 実行時間×スキュー比率散布図・ステージ別実行時間棒グラフ・詳細テーブル |
| **Executor 分析** | 負荷分布棒グラフ・詳細テーブル |
| **スキュー分析** | 時間スキューギャップ棒グラフ・データ量スキューギャップ棒グラフ・散布図・詳細テーブル |
| **SQL / Photon 分析** | Photon率カウンター（全体・上位5%・上位10%）・オペレータ数・Join種別カウンター・SQLテーブル |
| **Shuffle 分析** | Read/Write/FetchWait棒グラフ・Executor別Shuffle棒グラフ・詳細テーブル |
| **並列実行分析** | 同時実行数・CPU効率カウンター・散布図・棒グラフ・スケジューリングディレイ棒グラフ・詳細テーブル |

### ダッシュボード作成・更新

同名ダッシュボードを自動検出して PATCH で上書き更新するため、**URLは変わりません**。

Databricks ワークスペースの `create_dashboard_notebook` を開いて実行します。先頭の CONFIGURATION セルを環境に合わせて変更してください。

| パラメータ | 説明 |
|---|---|
| `SCHEMA` | Gold テーブルが格納されているカタログ.スキーマ（例: `main.base2`） |
| `WAREHOUSE_ID` | ダッシュボードのクエリを実行する SQL ウェアハウス ID |
| `PARENT_PATH` | ダッシュボードファイルを保存するワークスペースパス |
| `DASH_NAME` | ダッシュボードの表示名 |
| `EXISTING_DASHBOARD_ID` | 通常は `None` のまま（名前で自動検出）。明示的にIDを指定することも可 |

> **Note:** ワークスペースAPIで `PARENT_PATH/DASH_NAME.lvdash.json` を直接検索し、存在すれば PATCH で中身を上書きします。ダッシュボードの URL は更新のたびに変わりません。

---

## テスト方法

### Step 1: サンプルデータ生成

`generate_test_data.py` を Databricks ノートブックとして開き、`LOG_ROOT` と `CLUSTER_ID` を変更して実行します：

| ステージ | シナリオ |
|---|---|
| Stage 0 | 正常 (OK) |
| Stage 1 | データスキュー（1タスクが中央値の50倍遅い） |
| Stage 2 | Disk Spill（10 GB） |
| Stage 3 | High GC（GC オーバーヘッド 28%） |
| Stage 4 | Heavy Shuffle（15 GB shuffle read） |
| Stage 5 | ステージ失敗 |

### Step 2: DLT パイプラインを Development モードで実行

### Step 3: 結果確認

```sql
-- ボトルネック一覧
SELECT bottleneck_type, severity, stage_name, recommendation
FROM gold_bottleneck_report
ORDER BY severity;

-- スキューが大きいステージ
SELECT stage_id, task_skew_ratio, time_skew_gap_ms, data_skew_gap_mb
FROM gold_stage_performance
ORDER BY task_skew_ratio DESC NULLS LAST;

-- Photon 利用率・非対応オペレータ
SELECT description_short, duration_sec, photon_pct, non_photon_op_list
FROM gold_sql_photon_analysis
ORDER BY duration_sec DESC;

-- 並列実行によるCPU競合
SELECT job_id, concurrent_jobs_at_start, job_cpu_efficiency_pct, duration_sec
FROM gold_job_concurrency
ORDER BY concurrent_jobs_at_start DESC;
```

---

## ファイル構成

```
spark-perf-dlt/
├── pipeline.py                      # DLT パイプライン本体 (Bronze / Silver / Gold)
├── create_dashboard_notebook.py     # Lakeview ダッシュボード作成ノートブック（Databricks ワークスペース用）
├── generate_summary_notebook.py     # LLM による AI サマリー生成ノートブック（Workflow から自動トリガー）
├── generate_test_data.py            # テスト用サンプルデータ生成スクリプト
└── README.md                        # このファイル
```
