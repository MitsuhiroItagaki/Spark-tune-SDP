# Databricks notebook source

# MAGIC %md
# MAGIC # Spark Job Performance Analysis — ダッシュボード作成
# MAGIC
# MAGIC このノートブックを実行するとLakeviewダッシュボードを作成・公開します。
# MAGIC
# MAGIC **手順:**
# MAGIC 1. 下の「CONFIGURATION」セルを環境に合わせて変更する
# MAGIC 2. 「Run All」でノートブック全体を実行する
# MAGIC
# MAGIC **更新時:** `EXISTING_DASHBOARD_ID` は通常 `None` のままでOK。同名ダッシュボードを自動検索してPATCHで上書きするため、URLは変わりません。

# COMMAND ----------

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURATION — 環境に合わせて変更してください                          │
# └─────────────────────────────────────────────────────────────────────────┘

# Goldテーブルが格納されているカタログ.スキーマ
# 例: "main.base2"、"hive_metastore.spark_perf"
SCHEMA       = "main.base2"

# ダッシュボードのクエリを実行するSQLウェアハウスのID
# 確認方法: Databricks UI → SQL → SQL Warehouses → ウェアハウス名をクリック → 「接続の詳細」タブ
WAREHOUSE_ID = "bec52b183a4cfe2a"

# ダッシュボードファイルを保存するワークスペースパス（末尾スラッシュ不要）
# 例: "/Users/yourname@company.com/spark-perf-dlt"、"/Shared/spark-perf"
PARENT_PATH  = "/Users/mitsuhiro.itagaki@databricks.com/spark-perf-dlt"

# Lakeviewダッシュボードの表示名
# 同名ダッシュボードが PARENT_PATH に存在する場合は自動削除してから再作成する
DASH_NAME    = "Spark Job Performance Analysis"

# ──────────────────────────────────────────────────────────────────────────
# 更新オプション（通常は None のまま）
#
#   None        : DASH_NAME と PARENT_PATH で同名ダッシュボードを自動検索し、
#                 存在すれば PATCH で中身を上書き（IDとURLを引き継ぐ）
#                 見つからなければ新規作成する
#   "<dashboard_id>" : 指定IDのダッシュボードを PATCH で上書きする
#                 IDはダッシュボードのURLから取得可能:
#                 https://<host>/dashboardsv3/<dashboard_id>/published
# ──────────────────────────────────────────────────────────────────────────
EXISTING_DASHBOARD_ID = None

# COMMAND ----------

# ── インポート & LakeviewDashboard クラス（依存ライブラリなし） ──────────────
import json, uuid, requests
from typing import Optional, List, Dict, Any

class LakeviewDashboard:
    DEFAULT_COLORS = ["#FFAB00","#00A972","#FF3621","#8BCAE7","#AB4057","#99DDB4","#FCA4A1","#919191","#BF7080"]

    def __init__(self, name: str = "New Dashboard"):
        self.name = name
        self.datasets: List[Dict] = []
        self.pages: List[Dict] = []
        self._current_page: Optional[Dict] = None
        self.add_page("Overview")

    @staticmethod
    def _generate_id() -> str:
        return uuid.uuid4().hex[:8]

    def add_dataset(self, name, display_name, query):
        self.datasets.append({"name": name, "displayName": display_name, "queryLines": [query]})
        return name

    def add_page(self, display_name):
        page_id = self._generate_id()
        page = {"name": page_id, "displayName": display_name, "pageType": "PAGE_TYPE_CANVAS", "layout": []}
        self.pages.append(page)
        self._current_page = page
        return page_id

    def _add_widget(self, widget, position):
        self._current_page["layout"].append({"widget": widget, "position": {
            "x": position.get("x", 0), "y": position.get("y", 0),
            "width": position.get("width", 2), "height": position.get("height", 3)
        }})

    def add_counter(self, dataset_name, value_field, value_agg="SUM", title=None, position=None):
        wid = self._generate_id()
        if value_agg == "COUNT":
            vname, vexpr = "count(*)", "COUNT(`*`)"
        else:
            vname, vexpr = f"{value_agg.lower()}({value_field})", f"{value_agg}(`{value_field}`)"
        widget = {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name,
                "fields": [{"name": vname, "expression": vexpr}],
                "disaggregated": True
            }}],
            "spec": {
                "version": 2, "widgetType": "counter",
                "encodings": {"value": {"fieldName": vname, "displayName": title or vname}},
                "frame": {"showTitle": title is not None, "title": title or ""}
            }
        }
        self._add_widget(widget, position or {"x": 0, "y": 0, "width": 1, "height": 2})
        return wid

    def to_dict(self):
        return {
            "datasets": self.datasets,
            "pages": self.pages,
            "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False}
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    def get_api_payload(self, warehouse_id, parent_path):
        return {"display_name": self.name, "warehouse_id": warehouse_id,
                "parent_path": parent_path, "serialized_dashboard": self.to_json()}

# COMMAND ----------

# ── ヘルパー関数 ──────────────────────────────────────────────────────────────
def uid():
    return uuid.uuid4().hex[:8]

BN_COLORS     = ["#00A972","#FFAB00","#FF3621","#AB4057","#8BCAE7","#FCA4A1","#99DDB4","#919191"]
STATUS_COLORS = ["#00A972","#FF3621","#FFAB00"]

def add_widget(db, page_idx, widget, x, y, w, h):
    db.pages[page_idx]["layout"].append({
        "widget": widget,
        "position": {"x": x, "y": y, "width": w, "height": h}
    })

def agg_bar(ds_name, x_f, y_f, y_agg, color_f, title, colors=None, sort_x=None):
    wid = uid()
    y_name = f"{y_agg.lower()}({y_f})"
    y_expr = f"{y_agg}(`{y_f}`)"
    x_scale = {"type": "categorical"}
    if sort_x:
        x_scale["sort"] = {"by": sort_x}
    spec = {
        "version": 3, "widgetType": "bar",
        "encodings": {
            "x":     {"fieldName": x_f,    "scale": x_scale,                  "displayName": x_f},
            "y":     {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_f},
            "color": {"fieldName": color_f, "scale": {"type": "categorical"}, "displayName": color_f},
            "label": {"show": True},
        },
        "frame": {"showTitle": True, "title": title},
    }
    if colors:
        spec["mark"] = {"colors": colors}
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [
                {"name": x_f,    "expression": f"`{x_f}`"},
                {"name": y_name, "expression": y_expr},
                {"name": color_f,"expression": f"`{color_f}`"},
            ],
            "disaggregated": False
        }}],
        "spec": spec,
    }

def raw_scatter(ds_name, x_f, x_label, y_f, y_label, color_f, title, colors=None):
    wid = uid()
    spec = {
        "version": 3, "widgetType": "scatter",
        "encodings": {
            "x":     {"fieldName": x_f,     "scale": {"type": "quantitative"}, "displayName": x_label},
            "y":     {"fieldName": y_f,     "scale": {"type": "quantitative"}, "displayName": y_label},
            "color": {"fieldName": color_f, "scale": {"type": "categorical"},  "displayName": color_f},
        },
        "frame": {"showTitle": True, "title": title},
    }
    if colors:
        spec["mark"] = {"colors": colors}
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [
                {"name": x_f,     "expression": f"`{x_f}`"},
                {"name": y_f,     "expression": f"`{y_f}`"},
                {"name": color_f, "expression": f"`{color_f}`"},
            ],
            "disaggregated": True,
        }}],
        "spec": spec,
    }

def make_text_widget(text_md):
    return {"name": uid(), "textbox_spec": text_md}

def make_table(ds_name, field_names, title):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [{"name": f, "expression": f"`{f}`"} for f in field_names],
            "disaggregated": True,
        }}],
        "spec": {
            "version": 2, "widgetType": "table",
            "encodings": {"columns": [{"fieldName": f} for f in field_names]},
            "frame": {"showTitle": True, "title": title},
        }
    }

# COMMAND ----------

# MAGIC %md ## ダッシュボード構築

# COMMAND ----------

# ── ダッシュボード & データセット ─────────────────────────────────────────────
db = LakeviewDashboard(DASH_NAME)

db.add_dataset("app_ds", "Application Summary",
    f"SELECT cluster_id, app_id, app_name, spark_user, start_ts, end_ts, duration_ms, duration_min, total_jobs, succeeded_jobs, failed_jobs, job_success_rate, total_stages, completed_stages, failed_stages, total_tasks, total_input_gb, total_shuffle_gb, total_spill_gb, stages_with_disk_spill, total_gc_time_ms, gc_overhead_pct, total_exec_run_ms FROM {SCHEMA}.gold_application_summary")

db.add_dataset("job_ds", "Job Performance",
    f"SELECT cluster_id, app_id, CAST(job_id AS STRING) AS job_id_str, status, submit_ts, complete_ts, duration_ms, duration_ms / 1000.0 AS duration_sec, duration_min, job_result, stage_ids FROM {SCHEMA}.gold_job_performance")

db.add_dataset("stage_ds", "Stage Performance",
    f"SELECT cluster_id, app_id, stage_id, CAST(stage_id AS STRING) AS stage_id_str, attempt_id, stage_name, status, failure_reason, num_tasks, task_count, failed_tasks, submission_ts, first_task_ts, completion_ts, duration_ms, scheduling_delay_ms, ROUND(scheduling_delay_ms / 1000.0, 2) AS scheduling_delay_sec, gc_overhead_pct, cpu_efficiency_pct, shuffle_read_mb, shuffle_write_mb, shuffle_fetch_wait_ms, ROUND(shuffle_fetch_wait_ms / 1000.0, 2) AS shuffle_fetch_wait_sec, disk_spill_mb, memory_spill_mb, task_min_ms, task_avg_ms, task_p50_ms, task_p75_ms, task_p95_ms, task_p99_ms, task_max_ms, task_skew_ratio, time_skew_gap_ms, ROUND(task_p50_ms / 1000.0, 3) AS task_p50_sec, ROUND(task_max_ms / 1000.0, 3) AS task_max_sec, ROUND(time_skew_gap_ms / 1000.0, 3) AS time_skew_gap_sec, task_shuffle_min_mb, task_shuffle_p50_mb, task_shuffle_max_mb, shuffle_skew_ratio, data_skew_gap_mb, bottleneck_type, severity, recommendation FROM {SCHEMA}.gold_stage_performance")

db.add_dataset("exec_ds", "Executor Analysis",
    f"SELECT cluster_id, app_id, executor_id, host, total_cores, resource_profile_id, onheap_memory_mb, offheap_memory_mb, task_cpus, add_ts, remove_ts, removed_reason, total_tasks, total_task_ms, avg_task_ms, avg_task_ms / 1000.0 AS avg_task_sec, total_gc_ms, avg_gc_pct, avg_cpu_efficiency_pct, input_gb, shuffle_read_gb, shuffle_write_gb, total_memory_spilled, total_disk_spilled, peak_memory_mb, app_avg_task_ms, load_vs_avg, z_score, CAST(is_straggler AS STRING) AS is_straggler_str, CAST(is_underutilized AS STRING) AS is_underutilized_str FROM {SCHEMA}.gold_executor_analysis")

db.add_dataset("jc_ds", "Job Concurrency",
    f"SELECT cluster_id, app_id, job_id, job_id_str, status, job_result, submit_ts, complete_ts, duration_ms, duration_sec, duration_min, concurrent_jobs_at_start, job_total_tasks, total_cpu_time_sec, total_exec_run_time_sec, job_cpu_efficiency_pct, avg_task_cpu_time_ms, total_gc_time_sec FROM {SCHEMA}.gold_job_concurrency ORDER BY submit_ts")

db.add_dataset("bn_ds", "Bottleneck Report",
    f"SELECT cluster_id, app_id, job_id, stage_id, stage_name, status, severity, bottleneck_type, duration_ms, num_tasks, task_skew_ratio, gc_overhead_pct, disk_spill_mb, memory_spill_mb, shuffle_read_mb, task_p95_ms, task_p99_ms, recommendation, failure_reason FROM {SCHEMA}.gold_bottleneck_report")

db.add_dataset("sql_ds", "SQL Photon Analysis",
    f"SELECT cluster_id, app_id, execution_id, description_short, start_ts, duration_sec, total_operators, photon_operators, photon_pct, is_photon, bhj_count, photon_bhj_count, smj_count, total_join_count, non_photon_op_list FROM {SCHEMA}.gold_sql_photon_analysis ORDER BY duration_sec DESC NULLS LAST")

db.add_dataset("sql_top5_ds", "SQL Top 5% Photon",
    f"SELECT AVG(photon_pct) AS avg_photon_pct_top5 FROM ("
    f"  SELECT photon_pct, NTILE(20) OVER (ORDER BY duration_sec DESC) AS ventile"
    f"  FROM {SCHEMA}.gold_sql_photon_analysis WHERE duration_sec IS NOT NULL"
    f") WHERE ventile = 1")

db.add_dataset("sql_top10_ds", "SQL Top 10% Photon",
    f"SELECT AVG(photon_pct) AS avg_photon_pct_top10 FROM ("
    f"  SELECT photon_pct, NTILE(10) OVER (ORDER BY duration_sec DESC) AS decile"
    f"  FROM {SCHEMA}.gold_sql_photon_analysis WHERE duration_sec IS NOT NULL"
    f") WHERE decile = 1")

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: 概要
# ══════════════════════════════════════════════════════════════════════════════
db.pages[0]["displayName"] = "概要"

_SUMMARY_TEXT = """\
## 処理概要と主なボトルネック

**概要**: 697ジョブ / 743ステージ / 22,210タスク、成功率99.1%。入力232.7GBに対しShuffle合計356.9GB（入力量を超過）。

**① Shuffle過多（12ステージ、Low）** — 最大47GB/ステージのShuffleが発生。小テーブルのBroadcastHashJoinやAQEによるShuffleパーティション自動最適化を検討。

**② データスキュー（8ステージ、Medium）** — 最大スキュー比83倍。特定パーティションへの処理集中が遅延を引き起こす。RepartitionまたはSaltingで偏りを解消すること。

**③ ジョブ失敗（6件）** — 分析の結果、5件はAQEが不要ステージを自動キャンセルした正常動作。1件（Job 151）はジョブグループのキャンセルに伴う連鎖停止。重大な障害は検出されず。

**④ Photon利用率33%（155クエリ中51）** — 非Photonオペレータが混在し加速効果を制限。SQL/Photon分析ページで非対応オペレータを確認のこと。

**⑤ 並列実行（最大15ジョブ同時）** — 平均CPU効率76.4%は許容範囲。ただし15ジョブ同時起動時はクラスタ飽和に注意。\
"""
add_widget(db, 0, make_text_widget(_SUMMARY_TEXT), 0, 0, 6, 5)

_TOP3_TEXT = """\
## 改善インパクト TOP 3 ジョブ

---

**🥇 #1: Job 276 — 226秒 → 推定 15〜20秒（約10〜15倍の改善）**

ステージ1005: 145タスク、タスクp50=2.1秒、CPU効率82%、同時実行8ジョブ。
タスク1本は2秒で終わるが合計226秒かかっており、Executorが事実上1〜2台しか割り当てられていない状態。
専有リソースがあれば `145タスク × 2.1秒 ÷ 推定28Executor ≒ 11秒` で完了できる。

**改善策:** ① Job スケジューリングの優先度制御（Spark Fair Scheduler の pool 設定）
　　　　 ② このジョブ起動時の同時実行数を 3 以下に制限
　　　　 ③ クラスタの Autoscale 上限を引き上げ、Executor の枯渇を防ぐ

---

**🥈 #2: Job 280 — 167秒 → 推定 15秒（約11倍）**

ステージ1010: 120タスク、タスクp50=2.4秒、CPU効率75.7%（全ジョブ中最低）、同時実行11ジョブ。
Job 276と同種の処理だが並列数がさらに多く CPU 競合が深刻。CPU効率の低下はスレッド待機の兆候。

**改善策:** Job 276 と同様のスケジューリング改善に加え、JVM GC チューニング（-XX:+UseG1GC 等）を確認。

---

**🥉 #3: Job 150 — 136秒（単独実行にもかかわらず遅延、下流への波及が最大）**

ステージ663: 209タスク、タスクp50=18.3秒、Shuffle書き出し **82GB**、CPU効率92%。
単独起動・高CPU効率にもかかわらずデータ量が律速。82GBの中間データが下流ジョブ（Job 108: 47GB読み込み）の
HEAVY_SHUFFLE ボトルネックの根本原因となっており、連鎖的な遅延を生んでいる。

**改善策:** ① SELECT * → 必要列のみに絞りシャッフルデータ量を削減
　　　　 ② `spark.sql.shuffle.partitions` を 200→400〜800 に増やしタスクあたり負荷を分散
　　　　 ③ Delta Z-ORDERING / パーティショニングで下流読み込みを Shuffle レス化
　　　　 ④ Shuffle 前段でのフィルタ適用（早期プロジェクション・プレディケートプッシュダウン確認）\
"""
add_widget(db, 0, make_text_widget(_TOP3_TEXT), 0, 5, 6, 9)

db.add_counter("app_ds", "total_tasks",      "SUM", "合計タスク数",      {"x":0,"y":14,"width":2,"height":3})
db.add_counter("app_ds", "total_stages",     "SUM", "合計ステージ数",    {"x":2,"y":14,"width":2,"height":3})
db.add_counter("app_ds", "total_jobs",       "SUM", "合計ジョブ数",      {"x":4,"y":14,"width":2,"height":3})
db.add_counter("app_ds", "total_shuffle_gb", "SUM", "Shuffle (GB)",     {"x":0,"y":17,"width":2,"height":3})
db.add_counter("app_ds", "total_spill_gb",   "SUM", "Spill (GB)",       {"x":2,"y":17,"width":2,"height":3})
db.add_counter("app_ds", "job_success_rate", "AVG", "ジョブ成功率 (%)", {"x":4,"y":17,"width":2,"height":3})

job_bar = agg_bar("job_ds", "job_id_str", "duration_sec", "MAX", "status",
                  "ジョブ別実行時間 (秒、色=ステータス)", STATUS_COLORS)
add_widget(db, 0, job_bar, 0, 20, 3, 5)

bn_tbl = make_table("bn_ds", [
    "job_id", "stage_id", "recommendation", "num_tasks",
    "status", "bottleneck_type", "failure_reason",
], "ボトルネックレポート")
add_widget(db, 0, bn_tbl, 3, 20, 3, 5)

bn_dist_bar = agg_bar("bn_ds", "bottleneck_type", "stage_id", "COUNT", "severity",
                      "ボトルネック種別 分布 (Skew / Spill / Shuffle / GC)",
                      BN_COLORS, sort_x="y-reversed")
add_widget(db, 0, bn_dist_bar, 0, 25, 6, 5)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: ステージ分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("ステージ分析")

scatter = raw_scatter("stage_ds",
    "task_skew_ratio", "タスクスキュー比率",
    "duration_ms",     "実行時間 (ms)",
    "bottleneck_type", "実行時間 vs タスクスキュー (色=ボトルネック種別)",
    BN_COLORS)
add_widget(db, 1, scatter, 0, 0, 3, 5)

stage_bar = agg_bar("stage_ds", "stage_id_str", "duration_ms", "MAX", "bottleneck_type",
                    "ステージ別実行時間 (色=ボトルネック種別)", BN_COLORS)
add_widget(db, 1, stage_bar, 3, 0, 3, 5)

stage_tbl = make_table("stage_ds", [
    "stage_id_str", "recommendation", "disk_spill_mb", "duration_ms",
    "failed_tasks", "memory_spill_mb", "num_tasks", "shuffle_read_mb",
    "shuffle_write_mb", "stage_id", "scheduling_delay_ms", "task_count",
    "status", "attempt_id", "cpu_efficiency_pct", "gc_overhead_pct",
    "task_avg_ms", "task_max_ms", "task_min_ms", "task_p50_ms", "task_p75_ms",
    "task_p95_ms", "task_p99_ms", "task_skew_ratio", "app_id",
    "bottleneck_type", "failure_reason", "severity", "stage_name",
    "completion_ts", "first_task_ts", "submission_ts",
], "ステージパフォーマンス詳細")
add_widget(db, 1, stage_tbl, 0, 5, 6, 7)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: Executor 分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("Executor 分析")

exec_bar = agg_bar("exec_ds", "executor_id", "avg_task_sec", "MAX", "is_straggler_str",
                   "Executor別 平均タスク実行時間 (秒、赤=ストラグラー)",
                   ["#FF3621", "#00A972"])
add_widget(db, 2, exec_bar, 0, 0, 6, 5)

exec_tbl = make_table("exec_ds", [
    "executor_id", "host", "total_cores", "task_cpus",
    "resource_profile_id", "onheap_memory_mb", "offheap_memory_mb",
    "is_straggler_str", "is_underutilized_str", "removed_reason",
    "avg_task_sec", "total_tasks", "total_task_ms",
    "avg_gc_pct", "avg_cpu_efficiency_pct",
    "load_vs_avg", "z_score",
    "input_gb", "shuffle_read_gb", "shuffle_write_gb",
    "peak_memory_mb", "total_memory_spilled", "total_disk_spilled",
    "add_ts", "remove_ts",
], "Executor 詳細")
add_widget(db, 2, exec_tbl, 0, 5, 6, 6)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: スキュー分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("スキュー分析")

time_skew_bar = agg_bar("stage_ds", "stage_id_str", "time_skew_gap_sec", "MAX", "bottleneck_type",
                        "ステージ別 処理時間スキューギャップ (最大タスク − 中央値 秒)",
                        BN_COLORS, sort_x="y-reversed")
add_widget(db, 3, time_skew_bar, 0, 0, 3, 5)

data_skew_bar = agg_bar("stage_ds", "stage_id_str", "data_skew_gap_mb", "MAX", "bottleneck_type",
                        "ステージ別 データ量スキューギャップ (最大 − 最小 MB/タスク)",
                        BN_COLORS, sort_x="y-reversed")
add_widget(db, 3, data_skew_bar, 3, 0, 3, 5)

skew_scatter = raw_scatter("stage_ds",
    "shuffle_skew_ratio", "データ量スキュー比率 (max/p50)",
    "task_skew_ratio",    "処理時間スキュー比率 (max/p50)",
    "bottleneck_type",    "データ量スキュー vs 処理時間スキュー (色=ボトルネック種別)",
    BN_COLORS)
add_widget(db, 3, skew_scatter, 0, 5, 6, 5)

skew_tbl = make_table("stage_ds", [
    "stage_id_str", "stage_name", "bottleneck_type",
    "num_tasks",
    "task_p50_sec", "task_max_sec", "time_skew_gap_sec", "task_skew_ratio",
    "task_shuffle_min_mb", "task_shuffle_p50_mb", "task_shuffle_max_mb",
    "data_skew_gap_mb", "shuffle_skew_ratio",
], "スキュー詳細 (処理時間・データ量の偏り)")
add_widget(db, 3, skew_tbl, 0, 10, 6, 6)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5: SQL / Photon 分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("SQL / Photon 分析")

db.add_counter("sql_ds",       "photon_pct",             "AVG", "平均 Photon 率 (%)",        {"x":0,"y":0,"width":2,"height":3})
db.add_counter("sql_top5_ds",  "avg_photon_pct_top5",    "AVG", "上位5% 平均 Photon 率 (%)",  {"x":2,"y":0,"width":2,"height":3})
db.add_counter("sql_top10_ds", "avg_photon_pct_top10",   "AVG", "上位10% 平均 Photon 率 (%)", {"x":4,"y":0,"width":2,"height":3})
db.add_counter("sql_ds",       "photon_operators",       "SUM", "Photon 演算子 合計",          {"x":0,"y":3,"width":2,"height":3})
db.add_counter("sql_ds",       "total_operators",        "SUM", "全演算子 合計",                {"x":2,"y":3,"width":2,"height":3})
db.add_counter("sql_ds",       "smj_count",              "SUM", "SortMergeJoin 合計",          {"x":4,"y":3,"width":2,"height":3})
db.add_counter("sql_ds",       "bhj_count",              "SUM", "BroadcastHashJoin 合計",      {"x":0,"y":6,"width":3,"height":3})
db.add_counter("sql_ds",       "photon_bhj_count",       "SUM", "Photon BHJ 合計",             {"x":3,"y":6,"width":3,"height":3})

sql_tbl = make_table("sql_ds", [
    "execution_id", "description_short", "start_ts", "duration_sec",
    "photon_pct", "is_photon",
    "total_join_count", "photon_bhj_count", "bhj_count", "smj_count",
    "non_photon_op_list",
], "SQL クエリ別 Photon 利用率・ジョイン種別")
add_widget(db, 4, sql_tbl, 0, 9, 6, 8)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 6: Shuffle 分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("Shuffle 分析")

shuffle_read_bar = agg_bar("stage_ds", "stage_id_str", "shuffle_read_mb", "MAX", "bottleneck_type",
                           "ステージ別 Shuffle Read (MB)", BN_COLORS, sort_x="y-reversed")
add_widget(db, 5, shuffle_read_bar, 0, 0, 3, 5)

shuffle_write_bar = agg_bar("stage_ds", "stage_id_str", "shuffle_write_mb", "MAX", "bottleneck_type",
                            "ステージ別 Shuffle Write (MB)", BN_COLORS, sort_x="y-reversed")
add_widget(db, 5, shuffle_write_bar, 3, 0, 3, 5)

shuffle_wait_bar = agg_bar("stage_ds", "stage_id_str", "shuffle_fetch_wait_sec", "MAX", "bottleneck_type",
                           "ステージ別 Shuffle Fetch 待ち時間 (秒)", BN_COLORS, sort_x="y-reversed")
add_widget(db, 5, shuffle_wait_bar, 0, 5, 3, 5)

exec_shuffle_bar = agg_bar("exec_ds", "executor_id", "shuffle_read_gb", "MAX", "is_straggler_str",
                           "Executor別 Shuffle Read (GB) ─ 偏りの確認",
                           ["#FF3621", "#00A972"])
add_widget(db, 5, exec_shuffle_bar, 3, 5, 3, 5)

shuffle_tbl = make_table("stage_ds", [
    "stage_id_str", "stage_name", "bottleneck_type", "num_tasks",
    "shuffle_read_mb", "shuffle_write_mb", "shuffle_fetch_wait_sec",
    "task_shuffle_min_mb", "task_shuffle_p50_mb", "task_shuffle_max_mb",
    "shuffle_skew_ratio", "data_skew_gap_mb",
    "duration_ms", "severity", "recommendation",
], "Shuffle 詳細テーブル")
add_widget(db, 5, shuffle_tbl, 0, 10, 6, 7)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 7: 並列実行分析
# ══════════════════════════════════════════════════════════════════════════════
db.add_page("並列実行分析")

db.add_counter("jc_ds", "concurrent_jobs_at_start", "MAX", "最大同時実行ジョブ数",      {"x":0,"y":0,"width":2,"height":3})
db.add_counter("jc_ds", "concurrent_jobs_at_start", "AVG", "平均同時実行ジョブ数",      {"x":2,"y":0,"width":2,"height":3})
db.add_counter("jc_ds", "job_cpu_efficiency_pct",   "AVG", "平均 CPU 効率 (%)",         {"x":4,"y":0,"width":2,"height":3})
db.add_counter("stage_ds", "scheduling_delay_sec",  "MAX", "最大 Scheduling Delay (秒)", {"x":0,"y":3,"width":2,"height":3})
db.add_counter("jc_ds", "total_cpu_time_sec",       "SUM", "合計 CPU 時間 (秒)",         {"x":2,"y":3,"width":2,"height":3})
db.add_counter("jc_ds", "total_gc_time_sec",        "SUM", "合計 GC 時間 (秒)",          {"x":4,"y":3,"width":2,"height":3})

cpu_scatter = raw_scatter("jc_ds",
    "concurrent_jobs_at_start", "同時実行ジョブ数",
    "job_cpu_efficiency_pct",   "CPU 効率 (%)",
    "status", "同時実行数 vs CPU 効率 ─ 並列実行による CPU 競合の確認",
    STATUS_COLORS)
add_widget(db, 6, cpu_scatter, 0, 6, 3, 5)

duration_scatter = raw_scatter("jc_ds",
    "concurrent_jobs_at_start", "同時実行ジョブ数",
    "duration_sec",             "ジョブ実行時間 (秒)",
    "status", "同時実行数 vs 処理時間 ─ 並列実行による遅延の確認",
    STATUS_COLORS)
add_widget(db, 6, duration_scatter, 3, 6, 3, 5)

cpu_eff_bar = agg_bar("jc_ds", "job_id_str", "job_cpu_efficiency_pct", "MAX", "status",
                      "ジョブ別 CPU 効率 (%) ─ 低いほど CPU 競合・GC・待機が多い",
                      STATUS_COLORS, sort_x="y-reversed")
add_widget(db, 6, cpu_eff_bar, 0, 11, 3, 5)

concurrency_bar = agg_bar("jc_ds", "job_id_str", "concurrent_jobs_at_start", "MAX", "status",
                           "ジョブ別 開始時の同時実行ジョブ数",
                           STATUS_COLORS, sort_x="y-reversed")
add_widget(db, 6, concurrency_bar, 3, 11, 3, 5)

delay_bar = agg_bar("stage_ds", "stage_id_str", "scheduling_delay_sec", "MAX", "bottleneck_type",
                    "ステージ別 Scheduling Delay (秒) ─ エグゼキューター待ち時間",
                    BN_COLORS, sort_x="y-reversed")
add_widget(db, 6, delay_bar, 0, 16, 6, 5)

jc_tbl = make_table("jc_ds", [
    "job_id_str", "status", "job_result",
    "submit_ts", "complete_ts",
    "duration_sec", "concurrent_jobs_at_start",
    "job_total_tasks", "total_cpu_time_sec", "total_exec_run_time_sec",
    "job_cpu_efficiency_pct", "avg_task_cpu_time_ms", "total_gc_time_sec",
], "ジョブ別 並列実行・CPU 詳細")
add_widget(db, 6, jc_tbl, 0, 21, 6, 7)

print("ダッシュボード構築完了")

# COMMAND ----------

# MAGIC %md ## 作成 & 公開

# COMMAND ----------

# ── 認証情報を Databricks コンテキストから自動取得 ────────────────────────────
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ── 既存ダッシュボードを検索 ───────────────────────────────────────────────────
target_id = EXISTING_DASHBOARD_ID

if not target_id:
    # ワークスペースAPIでファイルパスを直接指定して resource_id (=dashboard_id) を取得
    dashboard_path = f"{PARENT_PATH}/{DASH_NAME}.lvdash.json"
    r = requests.get(f"{host}/api/2.0/workspace/get-status",
                     headers=headers, params={"path": dashboard_path})
    if r.status_code == 200 and r.json().get("object_type") == "DASHBOARD":
        target_id = r.json().get("resource_id")
        print(f"Found existing dashboard: {target_id}")

# ── ダッシュボードを作成 or 更新（IDを引き継いでURLを固定） ───────────────────
payload = db.get_api_payload(WAREHOUSE_ID, PARENT_PATH)
payload["display_name"] = DASH_NAME

if target_id:
    # 既存IDに対して PATCH → URLが変わらない
    r = requests.patch(f"{host}/api/2.0/lakeview/dashboards/{target_id}", headers=headers, json=payload)
    assert r.status_code == 200, f"UPDATE ERROR: {r.status_code} {r.text}"
    dashboard_id = target_id
    print(f"Updated: {dashboard_id}")
else:
    # 初回作成
    r = requests.post(f"{host}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
    assert r.status_code == 200, f"CREATE ERROR: {r.status_code} {r.text}"
    dashboard_id = r.json()["dashboard_id"]
    print(f"Created: {dashboard_id}")

# ── 公開 ──────────────────────────────────────────────────────────────────────
r = requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    headers=headers,
    json={"warehouse_id": WAREHOUSE_ID}
)
assert r.status_code == 200, f"PUBLISH ERROR: {r.status_code} {r.text}"

workspace_host = host.rstrip("/")
print("Published!")
print(f"URL: {workspace_host}/dashboardsv3/{dashboard_id}/published")
