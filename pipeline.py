# ==============================================================================
# DLT Pipeline: Spark Job Performance Analysis
# Data Source : Spark event logs (S3 / UC Volume)
# Processing  : Batch
# Language    : Python
#
# Pipeline Parameters (set in DLT pipeline settings):
#   pipeline.log_root        : ログのルートパス  例) s3://bucket/cluster-logs
#                                                例) /Volumes/catalog/schema/volume/cluster-logs
#   pipeline.cluster_id      : 対象クラスタの ID  例) 0123-456789-xxxxx
#                              → 実際の読み込みパス: {log_root}/{cluster_id}/eventlog/
#   pipeline.secret_scope    : Databricks Secret scope 名 (default: spark-perf)
#   pipeline.secret_key      : Secret key 名            (default: databricks-token)
#
# アーキテクチャ:
#   Bronze  : Raw テキスト行 (1 table)
#   Silver  : イベント種別ごとにパース (6 tables)
#   Gold    : 集計・分析・ボトルネック判定 (5 tables)
# ==============================================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ==============================================================================
# CONFIGURATION
# ==============================================================================
_LOG_ROOT      = spark.conf.get("pipeline.log_root").rstrip("/")
CLUSTER_ID     = spark.conf.get("pipeline.cluster_id")
EVENT_LOG_PATH = f"{_LOG_ROOT}/{CLUSTER_ID}/eventlog/"

# ==============================================================================
# SCHEMA DEFINITIONS
# ==============================================================================

# Accumulables の各アイテム (SparkListenerStageCompleted の中に含まれる)
_ACCUM_ITEM = StructType([
    StructField("ID",                   LongType()),
    StructField("Name",                 StringType()),
    StructField("Value",                StringType()),
    StructField("Internal",             BooleanType()),
    StructField("Count Failed Values",  BooleanType()),
])

_STAGE_INFO = StructType([
    StructField("Stage ID",                 IntegerType()),
    StructField("Stage Attempt ID",         IntegerType()),
    StructField("Stage Name",               StringType()),
    StructField("Number of Tasks",          IntegerType()),
    StructField("Submission Time",          LongType()),
    StructField("First Task Launched Time", LongType()),
    StructField("Completion Time",          LongType()),
    StructField("Failure Reason",           StringType()),
    StructField("Accumulables",             ArrayType(_ACCUM_ITEM)),
])

_TASK_INFO = StructType([
    StructField("Task ID",              LongType()),
    StructField("Index",                IntegerType()),
    StructField("Attempt",              IntegerType()),
    StructField("Launch Time",          LongType()),
    StructField("Executor ID",          StringType()),
    StructField("Host",                 StringType()),
    StructField("Locality",             StringType()),
    StructField("Speculative",          BooleanType()),
    StructField("Getting Result Time",  LongType()),
    StructField("Finish Time",          LongType()),
])

_TASK_METRICS = StructType([
    StructField("Executor Deserialize Time",        LongType()),
    StructField("Executor Deserialize CPU Time",    LongType()),
    StructField("Executor Run Time",                LongType()),
    StructField("Executor CPU Time",                LongType()),
    StructField("Peak Execution Memory",            LongType()),
    StructField("Result Size",                      LongType()),
    StructField("JVM GC Time",                      LongType()),
    StructField("Result Serialization Time",        LongType()),
    StructField("Memory Bytes Spilled",             LongType()),
    StructField("Disk Bytes Spilled",               LongType()),
    StructField("Shuffle Read Metrics", StructType([
        StructField("Remote Blocks Fetched",    LongType()),
        StructField("Local Blocks Fetched",     LongType()),
        StructField("Fetch Wait Time",          LongType()),
        StructField("Remote Bytes Read",        LongType()),
        StructField("Remote Bytes Read To Disk",LongType()),
        StructField("Local Bytes Read",         LongType()),
        StructField("Total Records Read",       LongType()),
    ])),
    StructField("Shuffle Write Metrics", StructType([
        StructField("Shuffle Bytes Written",    LongType()),
        StructField("Shuffle Write Time",       LongType()),
        StructField("Shuffle Records Written",  LongType()),
    ])),
    StructField("Input Metrics", StructType([
        StructField("Bytes Read",       LongType()),
        StructField("Records Read",     LongType()),
    ])),
    StructField("Output Metrics", StructType([
        StructField("Bytes Written",    LongType()),
        StructField("Records Written",  LongType()),
    ])),
])

_TASK_END_EVENT = StructType([
    StructField("Event",            StringType()),
    StructField("Stage ID",         IntegerType()),
    StructField("Stage Attempt ID", IntegerType()),
    StructField("Task Type",        StringType()),
    StructField("Task End Reason",  StructType([StructField("Reason", StringType())])),
    StructField("Task Info",        _TASK_INFO),
    StructField("Task Metrics",     _TASK_METRICS),
])


# ==============================================================================
# HELPER: Accumulables から指定メトリクス名の値を取得
# ==============================================================================
def _accum(arr_col, metric_name: str):
    """
    Stage Info の Accumulables 配列から内部メトリクスを抽出。
    該当なしの場合は 0 を返す。
    """
    matched = F.filter(arr_col, lambda x: x["Name"] == metric_name)
    return F.when(
        F.size(matched) > 0,
        matched[0]["Value"].cast(LongType())
    ).otherwise(F.lit(0).cast(LongType()))


# ==============================================================================
# BRONZE LAYER
# ==============================================================================

@dlt.table(
    name="bronze_raw_events",
    comment="S3 / UC Volume から読み込んだ Spark event log の生テキスト行。1行1イベント。",
    table_properties={"quality": "bronze"},
)
def bronze_raw_events():
    return (
        spark.read
        # 実行中クラスタの .inprogress ファイルも対象に含める
        .option("recursiveFileLookup", "true")
        .text(EVENT_LOG_PATH)
        .withColumn("source_file",  F.col("_metadata.file_path"))
        # ファイルパスから app_id を抽出
        # Databricks 形式: .../eventlog/{app_id}/{run_id}/eventlog...
        # 標準 Spark 形式: .../eventlog/application_XXXXX_NNNN
        .withColumn("app_id",
            F.coalesce(
                # Databricks 独自形式: /eventlog/ の直後のディレクトリ名
                F.nullif(
                    F.regexp_extract(F.col("source_file"), r"/eventlog/([^/]+)/", 1),
                    F.lit("")
                ),
                # 標準 Spark 形式: application_XXXXX_NNNN
                F.nullif(
                    F.regexp_extract(F.col("source_file"), r"(application_\d+_\d+)", 1),
                    F.lit("")
                ),
            )
        )
        .withColumn("event_type",   F.get_json_object(F.col("value"), "$.Event"))
        .withColumn("cluster_id",   F.lit(CLUSTER_ID))
        .withColumn("ingested_at",  F.current_timestamp())
        # 空行・非JSONは除外
        .filter(F.col("event_type").isNotNull())
    )


# ==============================================================================
# SILVER LAYER  ─  イベント種別ごとにパース・フラット化
# ==============================================================================

# ------------------------------------------------------------
# 1. Application Start / End
# ------------------------------------------------------------
@dlt.table(
    name="silver_application_events",
    comment="アプリケーションの開始・終了イベント",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_app_id", "app_id IS NOT NULL AND app_id != ''")
def silver_application_events():
    j = lambda p: F.get_json_object(F.col("value"), p)
    return (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type").isin(
            "SparkListenerApplicationStart",
            "SparkListenerApplicationEnd"
        ))
        .withColumn("app_name",     j("$['App Name']"))
        .withColumn("spark_user",   j("$['Spark User']"))
        .withColumn("timestamp_ms", j("$.Timestamp").cast("long"))
        .withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp_ms") / 1000))
        .select(
            "cluster_id", "app_id", "event_type",
            "app_name", "spark_user",
            "timestamp_ts", "timestamp_ms",
            "source_file", "ingested_at",
        )
    )


# ------------------------------------------------------------
# 2. Job Start / End
# ------------------------------------------------------------
@dlt.table(
    name="silver_job_events",
    comment="ジョブの開始・終了イベント",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_job_id", "job_id IS NOT NULL")
def silver_job_events():
    j = lambda p: F.get_json_object(F.col("value"), p)
    return (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type").isin(
            "SparkListenerJobStart",
            "SparkListenerJobEnd"
        ))
        .withColumn("job_id",           j("$['Job ID']").cast("int"))
        .withColumn("submission_ms",    j("$['Submission Time']").cast("long"))
        .withColumn("completion_ms",    j("$['Completion Time']").cast("long"))
        .withColumn("job_result",       j("$['Job Result']['Result']"))
        .withColumn("stage_ids",        j("$['Stage IDs']"))   # JSON string of array
        .withColumn("submission_ts",    F.to_timestamp(F.col("submission_ms") / 1000))
        .withColumn("completion_ts",    F.to_timestamp(F.col("completion_ms") / 1000))
        .select(
            "cluster_id", "app_id", "event_type", "job_id",
            "submission_ts", "completion_ts",
            "submission_ms", "completion_ms",
            "job_result", "stage_ids",
            "source_file", "ingested_at",
        )
    )


# ------------------------------------------------------------
# 3. Stage Completed  ─  Accumulables から集計メトリクスを抽出
# ------------------------------------------------------------
@dlt.table(
    name="silver_stage_events",
    comment="ステージ完了イベント。Accumulables から I/O・Shuffle・GC・Spill メトリクスを抽出。",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_stage_id", "stage_id IS NOT NULL")
@dlt.expect("has_tasks", "num_tasks > 0")
def silver_stage_events():
    _stage_event_schema = StructType([
        StructField("Event",      StringType()),
        StructField("Stage Info", _STAGE_INFO),
    ])

    df = (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") == "SparkListenerStageCompleted")
        .withColumn("p",   F.from_json(F.col("value"), _stage_event_schema))
        .withColumn("si",  F.col("p.Stage Info"))
        .withColumn("acc", F.col("si.Accumulables"))
    )

    return (
        df
        # Stage Info 基本フィールド
        .withColumn("stage_id",         F.col("si.Stage ID"))
        .withColumn("attempt_id",       F.col("si.Stage Attempt ID"))
        .withColumn("stage_name",       F.col("si.Stage Name"))
        .withColumn("num_tasks",        F.col("si.Number of Tasks"))
        .withColumn("submission_ms",    F.col("si.Submission Time"))
        .withColumn("first_task_ms",    F.col("si.First Task Launched Time"))
        .withColumn("completion_ms",    F.col("si.Completion Time"))
        .withColumn("failure_reason",   F.col("si.Failure Reason"))
        .withColumn("status",
            F.when(
                F.col("failure_reason").isNotNull() & (F.col("failure_reason") != ""),
                F.lit("FAILED")
            ).otherwise(F.lit("COMPLETED"))
        )
        # タイムスタンプ変換
        .withColumn("submission_ts",    F.to_timestamp(F.col("submission_ms") / 1000))
        .withColumn("first_task_ts",    F.to_timestamp(F.col("first_task_ms") / 1000))
        .withColumn("completion_ts",    F.to_timestamp(F.col("completion_ms") / 1000))
        .withColumn("duration_ms",      F.col("completion_ms") - F.col("submission_ms"))
        .withColumn("scheduling_delay_ms",
            F.when(
                F.col("first_task_ms").isNotNull(),
                F.col("first_task_ms") - F.col("submission_ms")
            ).otherwise(F.lit(0))
        )
        # --- Accumulables: 実行時間・CPU ---
        .withColumn("executor_run_time_ms",
            _accum(F.col("acc"), "internal.metrics.executorRunTime"))
        .withColumn("executor_cpu_time_ns",
            _accum(F.col("acc"), "internal.metrics.executorCpuTime"))
        .withColumn("jvm_gc_time_ms",
            _accum(F.col("acc"), "internal.metrics.jvmGarbageCollectionTime"))
        .withColumn("deserialize_ms",
            _accum(F.col("acc"), "internal.metrics.executorDeserializeTime"))
        .withColumn("result_serialize_ms",
            _accum(F.col("acc"), "internal.metrics.resultSerializationTime"))
        .withColumn("result_size_bytes",
            _accum(F.col("acc"), "internal.metrics.resultSize"))
        .withColumn("peak_exec_memory_bytes",
            _accum(F.col("acc"), "internal.metrics.peakExecutionMemory"))
        # --- Accumulables: Spill ---
        .withColumn("memory_bytes_spilled",
            _accum(F.col("acc"), "internal.metrics.memoryBytesSpilled"))
        .withColumn("disk_bytes_spilled",
            _accum(F.col("acc"), "internal.metrics.diskBytesSpilled"))
        # --- Accumulables: I/O ---
        .withColumn("input_bytes",
            _accum(F.col("acc"), "internal.metrics.input.bytesRead"))
        .withColumn("input_records",
            _accum(F.col("acc"), "internal.metrics.input.recordsRead"))
        .withColumn("output_bytes",
            _accum(F.col("acc"), "internal.metrics.output.bytesWritten"))
        .withColumn("output_records",
            _accum(F.col("acc"), "internal.metrics.output.recordsWritten"))
        # --- Accumulables: Shuffle Read ---
        .withColumn("shuffle_remote_bytes",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.remoteBytesRead"))
        .withColumn("shuffle_local_bytes",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.localBytesRead"))
        .withColumn("shuffle_read_records",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.recordsRead"))
        .withColumn("shuffle_fetch_wait_ms",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.fetchWaitTime"))
        .withColumn("shuffle_remote_blocks",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.remoteBlocksFetched"))
        .withColumn("shuffle_local_blocks",
            _accum(F.col("acc"), "internal.metrics.shuffle.read.localBlocksFetched"))
        # --- Accumulables: Shuffle Write ---
        .withColumn("shuffle_write_bytes",
            _accum(F.col("acc"), "internal.metrics.shuffle.write.bytesWritten"))
        .withColumn("shuffle_write_records",
            _accum(F.col("acc"), "internal.metrics.shuffle.write.recordsWritten"))
        .withColumn("shuffle_write_time_ns",
            _accum(F.col("acc"), "internal.metrics.shuffle.write.writeTime"))
        # --- 派生: Shuffle Read 合計 ---
        .withColumn("shuffle_read_bytes",
            F.col("shuffle_remote_bytes") + F.col("shuffle_local_bytes"))
        # --- 派生: GC オーバーヘッド率 ---
        .withColumn("gc_overhead_pct",
            F.when(
                F.col("executor_run_time_ms") > 0,
                F.col("jvm_gc_time_ms") / F.col("executor_run_time_ms") * 100
            ).otherwise(F.lit(0.0))
        )
        # --- 派生: CPU 効率 (Executor CPU / Executor Run Time) ---
        .withColumn("cpu_efficiency_pct",
            F.when(
                F.col("executor_run_time_ms") > 0,
                (F.col("executor_cpu_time_ns") / 1e6) / F.col("executor_run_time_ms") * 100
            ).otherwise(None)
        )
        .select(
            "cluster_id", "app_id", "stage_id", "attempt_id",
            "stage_name", "status", "failure_reason",
            "num_tasks",
            "submission_ts", "first_task_ts", "completion_ts",
            "duration_ms", "scheduling_delay_ms",
            # 実行時間・CPU・GC
            "executor_run_time_ms", "executor_cpu_time_ns",
            "jvm_gc_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
            "deserialize_ms", "result_serialize_ms", "result_size_bytes",
            "peak_exec_memory_bytes",
            # Spill
            "memory_bytes_spilled", "disk_bytes_spilled",
            # I/O
            "input_bytes", "input_records",
            "output_bytes", "output_records",
            # Shuffle
            "shuffle_read_bytes", "shuffle_read_records",
            "shuffle_remote_bytes", "shuffle_local_bytes",
            "shuffle_remote_blocks", "shuffle_local_blocks",
            "shuffle_fetch_wait_ms",
            "shuffle_write_bytes", "shuffle_write_records", "shuffle_write_time_ns",
            "source_file", "ingested_at",
        )
    )


# ------------------------------------------------------------
# 4. Task End  ─  タスク単位の詳細メトリクス
# ------------------------------------------------------------
@dlt.table(
    name="silver_task_events",
    comment="タスク完了イベント。タスク単位の実行時間・GC・Spill・Shuffle メトリクス。",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_task_id", "task_id IS NOT NULL")
@dlt.expect("has_metrics", "executor_run_time_ms IS NOT NULL")
def silver_task_events():
    df = (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") == "SparkListenerTaskEnd")
        .withColumn("p", F.from_json(F.col("value"), _TASK_END_EVENT))
    )

    ti = F.col("p.Task Info")
    tm = F.col("p.Task Metrics")
    sr = F.col("p.Task Metrics.Shuffle Read Metrics")
    sw = F.col("p.Task Metrics.Shuffle Write Metrics")
    im = F.col("p.Task Metrics.Input Metrics")
    om = F.col("p.Task Metrics.Output Metrics")

    return (
        df
        .withColumn("stage_id",         F.col("p.Stage ID"))
        .withColumn("attempt_id",       F.col("p.Stage Attempt ID"))
        .withColumn("task_type",        F.col("p.Task Type"))
        .withColumn("task_result",      F.col("p.Task End Reason.Reason"))
        # Task Info
        .withColumn("task_id",          ti["Task ID"])
        .withColumn("index",            ti["Index"])
        .withColumn("executor_id",      ti["Executor ID"])
        .withColumn("host",             ti["Host"])
        .withColumn("locality",         ti["Locality"])
        .withColumn("speculative",      ti["Speculative"])
        .withColumn("launch_ms",        ti["Launch Time"])
        .withColumn("finish_ms",        ti["Finish Time"])
        .withColumn("task_duration_ms", ti["Finish Time"] - ti["Launch Time"])
        .withColumn("launch_ts",        F.to_timestamp(ti["Launch Time"] / 1000))
        # Task Metrics
        .withColumn("executor_run_time_ms",     tm["Executor Run Time"])
        .withColumn("executor_cpu_time_ns",     tm["Executor CPU Time"])
        .withColumn("deserialize_ms",           tm["Executor Deserialize Time"])
        .withColumn("result_serialize_ms",      tm["Result Serialization Time"])
        .withColumn("gc_time_ms",               tm["JVM GC Time"])
        .withColumn("result_size_bytes",        tm["Result Size"])
        .withColumn("peak_exec_memory_bytes",   tm["Peak Execution Memory"])
        .withColumn("memory_bytes_spilled",     tm["Memory Bytes Spilled"])
        .withColumn("disk_bytes_spilled",       tm["Disk Bytes Spilled"])
        # Shuffle Read
        .withColumn("shuffle_remote_blocks",    sr["Remote Blocks Fetched"])
        .withColumn("shuffle_local_blocks",     sr["Local Blocks Fetched"])
        .withColumn("shuffle_fetch_wait_ms",    sr["Fetch Wait Time"])
        .withColumn("shuffle_remote_bytes",     sr["Remote Bytes Read"])
        .withColumn("shuffle_local_bytes",      sr["Local Bytes Read"])
        .withColumn("shuffle_read_records",     sr["Total Records Read"])
        .withColumn("shuffle_read_bytes",
            F.coalesce(sr["Remote Bytes Read"], F.lit(0)) +
            F.coalesce(sr["Local Bytes Read"],  F.lit(0))
        )
        # Shuffle Write
        .withColumn("shuffle_write_bytes",      sw["Shuffle Bytes Written"])
        .withColumn("shuffle_write_time_ns",    sw["Shuffle Write Time"])
        .withColumn("shuffle_write_records",    sw["Shuffle Records Written"])
        # Input / Output
        .withColumn("input_bytes",              im["Bytes Read"])
        .withColumn("input_records",            im["Records Read"])
        .withColumn("output_bytes",             om["Bytes Written"])
        .withColumn("output_records",           om["Records Written"])
        # 派生: GC オーバーヘッド率
        .withColumn("gc_overhead_pct",
            F.when(
                F.col("task_duration_ms") > 0,
                F.col("gc_time_ms") / F.col("task_duration_ms") * 100
            ).otherwise(F.lit(0.0))
        )
        # 派生: CPU 効率
        .withColumn("cpu_efficiency_pct",
            F.when(
                F.col("executor_run_time_ms") > 0,
                (F.col("executor_cpu_time_ns") / 1e6) / F.col("executor_run_time_ms") * 100
            ).otherwise(None)
        )
        .select(
            "cluster_id", "app_id", "stage_id", "attempt_id",
            "task_id", "index", "task_type", "task_result",
            "executor_id", "host", "locality", "speculative",
            "launch_ts", "task_duration_ms",
            "executor_run_time_ms", "executor_cpu_time_ns",
            "deserialize_ms", "result_serialize_ms",
            "gc_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
            "result_size_bytes", "peak_exec_memory_bytes",
            "memory_bytes_spilled", "disk_bytes_spilled",
            "shuffle_read_bytes", "shuffle_read_records",
            "shuffle_remote_bytes", "shuffle_local_bytes",
            "shuffle_remote_blocks", "shuffle_local_blocks",
            "shuffle_fetch_wait_ms",
            "shuffle_write_bytes", "shuffle_write_records", "shuffle_write_time_ns",
            "input_bytes", "input_records",
            "output_bytes", "output_records",
            "ingested_at",
        )
    )


# ------------------------------------------------------------
# 5. Executor Added / Removed
# ------------------------------------------------------------
@dlt.table(
    name="silver_executor_events",
    comment="Executor 追加・削除イベント",
    table_properties={"quality": "silver"},
)
def silver_executor_events():
    j = lambda p: F.get_json_object(F.col("value"), p)
    return (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type").isin(
            "SparkListenerExecutorAdded",
            "SparkListenerExecutorRemoved"
        ))
        .withColumn("executor_id",        j("$['Executor ID']"))
        .withColumn("timestamp_ms",       j("$.Timestamp").cast("long"))
        .withColumn("host",               j("$['Executor Info']['Host']"))
        .withColumn("total_cores",        j("$['Executor Info']['Total Cores']").cast("int"))
        .withColumn("removed_reason",     j("$['Removed Reason']"))
        .withColumn("resource_profile_id",j("$['Executor Info']['Resource Profile Id']").cast("int"))
        .withColumn("timestamp_ts",       F.to_timestamp(F.col("timestamp_ms") / 1000))
        .select(
            "cluster_id", "app_id", "event_type", "executor_id",
            "timestamp_ts", "host", "total_cores", "removed_reason",
            "resource_profile_id", "ingested_at",
        )
    )


# ------------------------------------------------------------
# 5b. Resource Profile (ResourceProfileAdded)
# ------------------------------------------------------------
@dlt.table(
    name="silver_resource_profiles",
    comment="Resource Profile ごとのメモリ設定 (OnHeap / OffHeap MB)",
    table_properties={"quality": "silver"},
)
def silver_resource_profiles():
    j = lambda p: F.get_json_object(F.col("value"), p)
    return (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") == "SparkListenerResourceProfileAdded")
        .withColumn("resource_profile_id",
            j("$['Resource Profile Id']").cast("int"))
        .withColumn("onheap_memory_mb",
            j("$['Executor Resource Requests']['memory']['Amount']").cast("double"))
        .withColumn("offheap_memory_mb",
            j("$['Executor Resource Requests']['offHeap']['Amount']").cast("double"))
        .withColumn("task_cpus",
            j("$['Task Resource Requests']['cpus']['Amount']").cast("double"))
        .select(
            "cluster_id", "app_id", "resource_profile_id",
            "onheap_memory_mb", "offheap_memory_mb", "task_cpus",
            "ingested_at",
        )
    )


# ------------------------------------------------------------
# 6. Spark Configuration (EnvironmentUpdate)
# ------------------------------------------------------------
@dlt.table(
    name="silver_spark_config",
    comment="パフォーマンス関連の Spark 設定値 (AQE, executor memory, shuffle 等)",
    table_properties={"quality": "silver"},
)
def silver_spark_config():
    _props_schema = ArrayType(ArrayType(StringType()))
    return (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") == "SparkListenerEnvironmentUpdate")
        .withColumn("spark_props",
            F.from_json(
                F.get_json_object(F.col("value"), "$['Spark Properties']"),
                _props_schema
            )
        )
        .withColumn("prop",         F.explode("spark_props"))
        .withColumn("config_key",   F.col("prop")[0])
        .withColumn("config_value", F.col("prop")[1])
        # パフォーマンス分析に関連する設定のみ絞り込み
        .filter(
            F.col("config_key").rlike(
                r"^spark\.(sql\.adaptive|executor|memory|cores"
                r"|shuffle|sql\.shuffle|sql\.broadcast"
                r"|databricks\.|sql\.files)"
            )
        )
        .select("cluster_id", "app_id", "config_key", "config_value", "ingested_at")
        .dropDuplicates(["cluster_id", "app_id", "config_key"])
    )


# ==============================================================================
# GOLD LAYER  ─  集計・分析・ダッシュボード用テーブル
# ==============================================================================

# ------------------------------------------------------------
# G1. Application Summary  ─  アプリ全体 KPI
# ------------------------------------------------------------
@dlt.table(
    name="gold_application_summary",
    comment="アプリケーション単位の全体 KPI。ダッシュボードのトップカード用。",
    table_properties={"quality": "gold"},
)
def gold_application_summary():
    app_start = (
        dlt.read("silver_application_events")
        .filter(F.col("event_type") == "SparkListenerApplicationStart")
        .select(
            "cluster_id", "app_id", "app_name", "spark_user",
            F.col("timestamp_ts").alias("start_ts"),
            F.col("timestamp_ms").alias("start_ms"),
        )
    )
    app_end = (
        dlt.read("silver_application_events")
        .filter(F.col("event_type") == "SparkListenerApplicationEnd")
        .select(
            "app_id",
            F.col("timestamp_ts").alias("end_ts"),
            F.col("timestamp_ms").alias("end_ms"),
        )
    )
    stage_agg = (
        dlt.read("silver_stage_events")
        .groupBy("cluster_id", "app_id")
        .agg(
            F.count("stage_id").alias("total_stages"),
            F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_stages"),
            F.sum(F.when(F.col("status") == "FAILED",    1).otherwise(0)).alias("failed_stages"),
            F.sum("num_tasks").alias("total_tasks"),
            F.sum("input_bytes").alias("total_input_bytes"),
            F.sum("shuffle_read_bytes").alias("total_shuffle_read_bytes"),
            F.sum("shuffle_write_bytes").alias("total_shuffle_write_bytes"),
            F.sum("disk_bytes_spilled").alias("total_disk_spilled_bytes"),
            F.sum("memory_bytes_spilled").alias("total_memory_spilled_bytes"),
            F.sum("jvm_gc_time_ms").alias("total_gc_time_ms"),
            F.sum("executor_run_time_ms").alias("total_exec_run_ms"),
            F.count(F.when(F.col("disk_bytes_spilled") > 0, 1)).alias("stages_with_disk_spill"),
        )
    )
    job_agg = (
        dlt.read("silver_job_events")
        .filter(F.col("event_type") == "SparkListenerJobEnd")
        .groupBy("cluster_id", "app_id")
        .agg(
            F.count("job_id").alias("total_jobs"),
            F.sum(F.when(F.col("job_result") == "JobSucceeded", 1).otherwise(0)).alias("succeeded_jobs"),
            F.sum(F.when(F.col("job_result") != "JobSucceeded", 1).otherwise(0)).alias("failed_jobs"),
        )
    )

    return (
        app_start
        .join(app_end,   on="app_id",                 how="left")
        .join(stage_agg, on=["cluster_id", "app_id"], how="left")
        .join(job_agg,   on=["cluster_id", "app_id"], how="left")
        .withColumn("duration_ms",      F.col("end_ms") - F.col("start_ms"))
        .withColumn("duration_min",     F.col("duration_ms") / 60_000)
        .withColumn("job_success_rate",
            F.when(F.col("total_jobs") > 0,
                F.round(F.col("succeeded_jobs") / F.col("total_jobs") * 100, 1)
            ).otherwise(None)
        )
        .withColumn("gc_overhead_pct",
            F.when(F.col("total_exec_run_ms") > 0,
                F.round(F.col("total_gc_time_ms") / F.col("total_exec_run_ms") * 100, 2)
            ).otherwise(0.0)
        )
        .withColumn("total_input_gb",   F.round(F.col("total_input_bytes")        / 1024**3, 3))
        .withColumn("total_shuffle_gb", F.round(F.col("total_shuffle_read_bytes") / 1024**3, 3))
        .withColumn("total_spill_gb",   F.round(
            (F.col("total_disk_spilled_bytes") + F.col("total_memory_spilled_bytes")) / 1024**3, 3
        ))
        .select(
            "cluster_id", "app_id", "app_name", "spark_user",
            "start_ts", "end_ts", "duration_ms", "duration_min",
            "total_jobs", "succeeded_jobs", "failed_jobs", "job_success_rate",
            "total_stages", "completed_stages", "failed_stages", "total_tasks",
            "total_input_gb", "total_shuffle_gb", "total_spill_gb",
            "stages_with_disk_spill",
            "total_gc_time_ms", "gc_overhead_pct",
            "total_exec_run_ms",
        )
    )


# ------------------------------------------------------------
# G2. Job Performance
# ------------------------------------------------------------
@dlt.table(
    name="gold_job_performance",
    comment="ジョブ単位の実行時間・成否。時系列可視化用。",
    table_properties={"quality": "gold"},
)
def gold_job_performance():
    job_start = (
        dlt.read("silver_job_events")
        .filter(F.col("event_type") == "SparkListenerJobStart")
        .select(
            "cluster_id", "app_id", "job_id", "stage_ids",
            F.col("submission_ts").alias("submit_ts"),
            F.col("submission_ms").alias("submit_ms"),
        )
    )
    job_end = (
        dlt.read("silver_job_events")
        .filter(F.col("event_type") == "SparkListenerJobEnd")
        .select(
            "app_id", "job_id", "job_result",
            F.col("completion_ts").alias("complete_ts"),
            F.col("completion_ms").alias("complete_ms"),
        )
    )
    return (
        job_start
        .join(job_end, on=["app_id", "job_id"], how="left")
        .withColumn("duration_ms",  F.col("complete_ms") - F.col("submit_ms"))
        .withColumn("duration_min", F.round(F.col("duration_ms") / 60_000, 2))
        .withColumn("status",
            F.when(F.col("job_result") == "JobSucceeded", F.lit("SUCCEEDED"))
             .when(F.col("job_result").isNotNull(),        F.lit("FAILED"))
             .otherwise(F.lit("RUNNING"))
        )
        .select(
            "cluster_id", "app_id", "job_id", "status",
            "submit_ts", "complete_ts", "duration_ms", "duration_min",
            "job_result", "stage_ids",
        )
        .orderBy("submit_ts")
    )


# ------------------------------------------------------------
# G3. Stage Performance  ─  ボトルネック分類・タスク分布付き
# ------------------------------------------------------------
@dlt.table(
    name="gold_stage_performance",
    comment="ステージ単位のパフォーマンス。タスク分布 (p50/p95/p99)・ボトルネック分類・推奨対応付き。",
    table_properties={"quality": "gold"},
)
def gold_stage_performance():
    stages = dlt.read("silver_stage_events")

    # タスク単位の分布統計 (成功タスクのみ)
    task_dist = (
        dlt.read("silver_task_events")
        .filter(F.col("task_result") == "Success")
        .groupBy("cluster_id", "app_id", "stage_id", "attempt_id")
        .agg(
            F.count("task_id").alias("task_count"),
            F.min("task_duration_ms").alias("task_min_ms"),
            F.max("task_duration_ms").alias("task_max_ms"),
            F.avg("task_duration_ms").alias("task_avg_ms"),
            F.percentile_approx("task_duration_ms", 0.50).alias("task_p50_ms"),
            F.percentile_approx("task_duration_ms", 0.75).alias("task_p75_ms"),
            F.percentile_approx("task_duration_ms", 0.95).alias("task_p95_ms"),
            F.percentile_approx("task_duration_ms", 0.99).alias("task_p99_ms"),
            F.percentile_approx("gc_time_ms",        0.95).alias("gc_p95_ms"),
            F.percentile_approx("peak_exec_memory_bytes", 0.95).alias("peak_mem_p95_bytes"),
            F.count(F.when(F.col("speculative"),              True)).alias("speculative_tasks"),
            F.count(F.when(F.col("disk_bytes_spilled") > 0,  True)).alias("tasks_with_disk_spill"),
            F.count(F.when(F.col("task_result") != "Success", True)).alias("failed_tasks"),
            # タスク単位のデータ量 (スキュー分析用)
            F.min("shuffle_read_bytes").alias("task_shuffle_min_bytes"),
            F.max("shuffle_read_bytes").alias("task_shuffle_max_bytes"),
            F.percentile_approx("shuffle_read_bytes", 0.50).alias("task_shuffle_p50_bytes"),
        )
    )

    return (
        stages
        .join(task_dist, on=["cluster_id", "app_id", "stage_id", "attempt_id"], how="left")
        # タスクスキュー比率 (max / p50)
        .withColumn("task_skew_ratio",
            F.when(F.col("task_p50_ms") > 0,
                F.round(F.col("task_max_ms").cast("double") / F.col("task_p50_ms"), 2)
            ).otherwise(None)
        )
        # 処理時間スキューギャップ (最大 − 中央値)
        .withColumn("time_skew_gap_ms",
            F.when(F.col("task_p50_ms").isNotNull(),
                F.col("task_max_ms") - F.col("task_p50_ms")
            ).otherwise(None)
        )
        # タスク単位データ量 MB 換算
        .withColumn("task_shuffle_min_mb",
            F.round(F.col("task_shuffle_min_bytes") / 1024**2, 2))
        .withColumn("task_shuffle_max_mb",
            F.round(F.col("task_shuffle_max_bytes") / 1024**2, 2))
        .withColumn("task_shuffle_p50_mb",
            F.round(F.col("task_shuffle_p50_bytes") / 1024**2, 2))
        # データ量スキュー比率 (max / p50)
        .withColumn("shuffle_skew_ratio",
            F.when(F.col("task_shuffle_p50_bytes") > 0,
                F.round(F.col("task_shuffle_max_bytes").cast("double") / F.col("task_shuffle_p50_bytes"), 2)
            ).otherwise(None)
        )
        # データ量スキューギャップ (最大 − 最小 per task)
        .withColumn("data_skew_gap_mb",
            F.round((F.col("task_shuffle_max_bytes") - F.col("task_shuffle_min_bytes")) / 1024**2, 2)
        )
        # データ量スキューギャップ (最大 − 中央値 per task)
        .withColumn("data_skew_gap_p50_mb",
            F.round((F.col("task_shuffle_max_bytes") - F.col("task_shuffle_p50_bytes")) / 1024**2, 2)
        )
        # MB 換算
        .withColumn("input_mb",         F.round(F.col("input_bytes")          / 1024**2, 1))
        .withColumn("output_mb",        F.round(F.col("output_bytes")         / 1024**2, 1))
        .withColumn("shuffle_read_mb",  F.round(F.col("shuffle_read_bytes")   / 1024**2, 1))
        .withColumn("shuffle_write_mb", F.round(F.col("shuffle_write_bytes")  / 1024**2, 1))
        .withColumn("disk_spill_mb",    F.round(F.col("disk_bytes_spilled")   / 1024**2, 1))
        .withColumn("memory_spill_mb",  F.round(F.col("memory_bytes_spilled") / 1024**2, 1))
        .withColumn("peak_mem_p95_mb",  F.round(F.col("peak_mem_p95_bytes")   / 1024**2, 1))
        .withColumn("has_disk_spill",   F.col("disk_bytes_spilled")   > 0)
        .withColumn("has_memory_spill", F.col("memory_bytes_spilled") > 0)
        # ─── ボトルネック分類 ───────────────────────────────────────────────
        .withColumn("bottleneck_type",
            F.when(F.col("status") == "FAILED",
                F.lit("STAGE_FAILURE"))
             .when(F.col("disk_bytes_spilled") > 0,
                F.lit("DISK_SPILL"))
             .when(F.col("gc_overhead_pct") > 20,
                F.lit("HIGH_GC"))
             .when(F.col("task_skew_ratio") > 5,
                F.lit("DATA_SKEW"))
             .when(F.col("shuffle_read_bytes") > 10 * 1024**3,   # > 10 GB
                F.lit("HEAVY_SHUFFLE"))
             .when(F.col("memory_bytes_spilled") > 0,
                F.lit("MEMORY_SPILL"))
             .when(F.col("gc_overhead_pct") > 10,
                F.lit("MODERATE_GC"))
             .otherwise(F.lit("OK"))
        )
        .withColumn("severity",
            F.when(F.col("bottleneck_type").isin("STAGE_FAILURE", "DISK_SPILL"), F.lit("HIGH"))
             .when(F.col("bottleneck_type").isin("HIGH_GC", "DATA_SKEW"),        F.lit("MEDIUM"))
             .when(F.col("bottleneck_type") == "OK",                             F.lit("NONE"))
             .otherwise(F.lit("LOW"))
        )
        # ─── 推奨アクション ─────────────────────────────────────────────────
        .withColumn("recommendation",
            F.when(F.col("bottleneck_type") == "STAGE_FAILURE",
                "failure_reason を確認。OOM の場合は executor memory を増加。")
             .when(F.col("bottleneck_type") == "DISK_SPILL",
                "executor memory を増加。AQE を有効化: spark.sql.adaptive.enabled=true")
             .when(F.col("bottleneck_type") == "HIGH_GC",
                "UDF・collect の見直し。オブジェクト生成を削減。G1GC チューニングを検討。")
             .when(F.col("bottleneck_type") == "DATA_SKEW",
                "AQE スキュー結合を有効化: spark.sql.adaptive.skewJoin.enabled=true または JOIN キーのサルティング。")
             .when(F.col("bottleneck_type") == "HEAVY_SHUFFLE",
                "Broadcast Join を検討 (spark.sql.autoBroadcastJoinThreshold)。shuffle partitions 数を調整。")
             .when(F.col("bottleneck_type") == "MEMORY_SPILL",
                "spark.memory.fraction を増加。repartition() でパーティションサイズを削減。")
             .when(F.col("bottleneck_type") == "MODERATE_GC",
                "GC 傾向を監視。executor memory の増加を検討。")
             .otherwise("ボトルネックなし。")
        )
        .select(
            "cluster_id", "app_id", "stage_id", "attempt_id",
            "stage_name", "status", "failure_reason",
            "num_tasks", "task_count", "failed_tasks",
            "submission_ts", "first_task_ts", "completion_ts",
            "duration_ms", "scheduling_delay_ms",
            # 実行・CPU・GC
            "executor_run_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
            "jvm_gc_time_ms",
            # I/O (MB)
            "input_mb", "output_mb",
            "shuffle_read_mb", "shuffle_write_mb", "shuffle_fetch_wait_ms",
            "disk_spill_mb", "memory_spill_mb",
            "has_disk_spill", "has_memory_spill",
            "peak_exec_memory_bytes",
            # タスク分布
            "task_min_ms", "task_avg_ms",
            "task_p50_ms", "task_p75_ms", "task_p95_ms", "task_p99_ms", "task_max_ms",
            "task_skew_ratio", "time_skew_gap_ms",
            # タスク単位データ量・データスキュー
            "task_shuffle_min_mb", "task_shuffle_p50_mb", "task_shuffle_max_mb",
            "shuffle_skew_ratio", "data_skew_gap_mb", "data_skew_gap_p50_mb",
            "gc_p95_ms", "peak_mem_p95_mb",
            "speculative_tasks", "tasks_with_disk_spill",
            # ボトルネック
            "bottleneck_type", "severity", "recommendation",
        )
    )


# ------------------------------------------------------------
# G4. Executor Analysis  ─  Executor 負荷分散・ストラグラー検出
# ------------------------------------------------------------
@dlt.table(
    name="gold_executor_analysis",
    comment="Executor 単位の負荷分散分析。ストラグラー・低稼働率 Executor の検出。",
    table_properties={"quality": "gold"},
)
def gold_executor_analysis():
    task_by_exec = (
        dlt.read("silver_task_events")
        .filter(F.col("task_result") == "Success")
        .groupBy("cluster_id", "app_id", "executor_id", "host")
        .agg(
            F.count("task_id").alias("total_tasks"),
            F.sum("task_duration_ms").alias("total_task_ms"),
            F.avg("task_duration_ms").alias("avg_task_ms"),
            F.sum("gc_time_ms").alias("total_gc_ms"),
            F.avg("gc_overhead_pct").alias("avg_gc_pct"),
            F.avg("cpu_efficiency_pct").alias("avg_cpu_efficiency_pct"),
            F.sum("input_bytes").alias("total_input_bytes"),
            F.sum("shuffle_read_bytes").alias("total_shuffle_read_bytes"),
            F.sum("shuffle_write_bytes").alias("total_shuffle_write_bytes"),
            F.sum("memory_bytes_spilled").alias("total_memory_spilled"),
            F.sum("disk_bytes_spilled").alias("total_disk_spilled"),
            F.max("peak_exec_memory_bytes").alias("peak_memory_bytes"),
            F.count(F.when(F.col("speculative"), True)).alias("speculative_tasks"),
        )
    )

    # アプリ内の Executor 平均・標準偏差（正規化用）
    app_norm = (
        task_by_exec
        .groupBy("cluster_id", "app_id")
        .agg(
            F.avg("total_task_ms").alias("app_avg_task_ms"),
            F.stddev("total_task_ms").alias("app_stddev_task_ms"),
        )
    )

    exec_lifecycle = (
        dlt.read("silver_executor_events")
        .groupBy("cluster_id", "app_id", "executor_id")
        .agg(
            F.max(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                         F.col("total_cores"))).alias("total_cores"),
            F.min(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                         F.col("timestamp_ts"))).alias("add_ts"),
            F.max(F.when(F.col("event_type") == "SparkListenerExecutorRemoved",
                         F.col("timestamp_ts"))).alias("remove_ts"),
            F.max(F.when(F.col("event_type") == "SparkListenerExecutorRemoved",
                         F.col("removed_reason"))).alias("removed_reason"),
            F.max(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                         F.col("resource_profile_id"))).alias("resource_profile_id"),
        )
    )

    resource_profiles = dlt.read("silver_resource_profiles")

    base = (
        task_by_exec
        .join(app_norm,       on=["cluster_id", "app_id"],                 how="left")
        .join(exec_lifecycle, on=["cluster_id", "app_id", "executor_id"],  how="left")
    )

    return (
        base
        .join(resource_profiles,
              on=["cluster_id", "app_id", "resource_profile_id"],
              how="left"
        )
        # アプリ平均に対する相対負荷
        .withColumn("load_vs_avg",
            F.when(F.col("app_avg_task_ms") > 0,
                F.round(F.col("total_task_ms") / F.col("app_avg_task_ms"), 2)
            ).otherwise(None)
        )
        # Z スコア（外れ値 Executor 検出）
        .withColumn("z_score",
            F.when(F.col("app_stddev_task_ms") > 0,
                F.round(
                    (F.col("total_task_ms") - F.col("app_avg_task_ms")) / F.col("app_stddev_task_ms"),
                    2
                )
            ).otherwise(None)
        )
        .withColumn("is_straggler",     F.col("load_vs_avg") > 1.5)
        .withColumn("is_underutilized", F.col("load_vs_avg") < 0.5)
        .withColumn("input_gb",         F.round(F.col("total_input_bytes")        / 1024**3, 3))
        .withColumn("shuffle_read_gb",  F.round(F.col("total_shuffle_read_bytes") / 1024**3, 3))
        .withColumn("shuffle_write_gb", F.round(F.col("total_shuffle_write_bytes")/ 1024**3, 3))
        .withColumn("peak_memory_mb",   F.round(F.col("peak_memory_bytes")        / 1024**2, 1))
        .select(
            "cluster_id", "app_id", "executor_id", "host",
            "total_cores", "add_ts", "remove_ts", "removed_reason",
            "resource_profile_id", "onheap_memory_mb", "offheap_memory_mb", "task_cpus",
            "total_tasks", "total_task_ms", "avg_task_ms",
            "total_gc_ms", "avg_gc_pct", "avg_cpu_efficiency_pct",
            "input_gb", "shuffle_read_gb", "shuffle_write_gb",
            "total_memory_spilled", "total_disk_spilled",
            "peak_memory_mb", "speculative_tasks",
            "app_avg_task_ms", "load_vs_avg", "z_score",
            "is_straggler", "is_underutilized",
        )
    )


# ------------------------------------------------------------
# G5. Bottleneck Report  ─  優先度付きボトルネック一覧
# ------------------------------------------------------------
@dlt.table(
    name="gold_bottleneck_report",
    comment="全ステージのボトルネックを重要度順にまとめたレポート。ダッシュボードのアクションリスト用。",
    table_properties={"quality": "gold"},
)
def gold_bottleneck_report():
    stages = dlt.read("gold_stage_performance")

    # ステージ→ジョブの紐付け
    _STAGE_IDS_SCHEMA = ArrayType(IntegerType())
    job_stage_map = (
        dlt.read("silver_job_events")
        .filter(F.col("event_type") == "SparkListenerJobStart")
        .withColumn("stage_id", F.explode(
            F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA)
        ))
        .select("cluster_id", "app_id", "job_id", "stage_id")
    )

    return (
        stages
        .filter(F.col("bottleneck_type") != "OK")
        .join(job_stage_map, on=["cluster_id", "app_id", "stage_id"], how="left")
        .withColumn("severity_order",
            F.when(F.col("severity") == "HIGH",   F.lit(1))
             .when(F.col("severity") == "MEDIUM",  F.lit(2))
             .otherwise(F.lit(3))
        )
        .select(
            "cluster_id", "app_id", "job_id",
            "stage_id", "stage_name", "status",
            "severity", "bottleneck_type",
            "duration_ms", "num_tasks",
            "task_skew_ratio", "gc_overhead_pct",
            "disk_spill_mb", "memory_spill_mb",
            "shuffle_read_mb",
            "task_p95_ms", "task_p99_ms",
            "recommendation", "failure_reason",
        )
        .orderBy("severity_order", F.col("duration_ms").desc())
    )


# ------------------------------------------------------------
# G7. Job Concurrency Analysis  ─  並列実行による高負荷分析
# ------------------------------------------------------------
@dlt.table(
    name="gold_job_concurrency",
    comment="各ジョブ開始時の同時実行ジョブ数と処理時間の相関。並列実行によるクラスタ高負荷の分析用。",
    table_properties={"quality": "gold"},
)
def gold_job_concurrency():
    jobs = dlt.read("gold_job_performance")

    # 自己結合: 全カラムに j2_ プレフィックスを付けた別 DataFrame
    j2 = jobs.toDF(*[f"j2_{c}" for c in jobs.columns])

    concurrency = (
        jobs
        .filter(F.col("submit_ts").isNotNull())
        .join(
            j2.filter(F.col("j2_submit_ts").isNotNull()),
            on=(
                (F.col("cluster_id")   == F.col("j2_cluster_id")) &
                (F.col("app_id")       == F.col("j2_app_id")) &
                (F.col("job_id")       != F.col("j2_job_id")) &
                # j2 が j1 の開始時点で実行中だった条件
                (F.col("j2_submit_ts")   <= F.col("submit_ts")) &
                (
                    F.col("j2_complete_ts").isNull() |
                    (F.col("j2_complete_ts") >= F.col("submit_ts"))
                )
            ),
            how="left"
        )
        .groupBy(
            "cluster_id", "app_id", "job_id",
            "status", "job_result",
            "submit_ts", "complete_ts",
            "duration_ms", "duration_min",
        )
        .agg(F.count("j2_job_id").alias("concurrent_jobs_at_start"))
    )

    # ── ジョブ単位の CPU 指標 ────────────────────────────────────────────
    _STAGE_IDS_SCHEMA = ArrayType(IntegerType())
    job_stage_map = (
        dlt.read("silver_job_events")
        .filter(F.col("event_type") == "SparkListenerJobStart")
        .withColumn("stage_id", F.explode(
            F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA)
        ))
        .select("cluster_id", "app_id", "job_id", "stage_id")
    )

    task_cpu = (
        dlt.read("silver_task_events")
        .filter(F.col("task_result") == "Success")
        .join(job_stage_map, on=["cluster_id", "app_id", "stage_id"], how="inner")
        .groupBy("cluster_id", "app_id", "job_id")
        .agg(
            F.count("task_id").alias("job_total_tasks"),
            F.round(F.sum("executor_cpu_time_ns") / 1e9,    2).alias("total_cpu_time_sec"),
            F.round(F.sum("executor_run_time_ms") / 1000.0, 1).alias("total_exec_run_time_sec"),
            F.round(
                F.sum("executor_cpu_time_ns") / 1e6
                / F.greatest(F.sum("executor_run_time_ms"), F.lit(1)) * 100,
                1
            ).alias("job_cpu_efficiency_pct"),
            F.round(F.avg("executor_cpu_time_ns") / 1e6, 1).alias("avg_task_cpu_time_ms"),
            F.round(F.sum("gc_time_ms") / 1000.0, 1).alias("total_gc_time_sec"),
        )
    )

    return (
        concurrency
        .join(task_cpu, on=["cluster_id", "app_id", "job_id"], how="left")
        .withColumn("job_id_str",   F.col("job_id").cast("string"))
        .withColumn("duration_sec", F.round(F.col("duration_ms") / 1000.0, 1))
        .select(
            "cluster_id", "app_id", "job_id", "job_id_str",
            "status", "job_result",
            "submit_ts", "complete_ts",
            "duration_ms", "duration_sec", "duration_min",
            "concurrent_jobs_at_start",
            # CPU 指標
            "job_total_tasks",
            "total_cpu_time_sec",
            "total_exec_run_time_sec",
            "job_cpu_efficiency_pct",
            "avg_task_cpu_time_ms",
            "total_gc_time_sec",
        )
    )


# ------------------------------------------------------------
# 7. SQL Execution Events  ─  Photon 利用率分析
# ------------------------------------------------------------
@dlt.table(
    name="silver_sql_executions",
    comment="SQL実行イベント (Start/End) の結合。物理プランを含む。",
    table_properties={"quality": "silver"},
)
def silver_sql_executions():
    j = lambda p: F.get_json_object(F.col("value"), p)

    starts = (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") ==
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
        .withColumn("execution_id",  j("$.executionId").cast("long"))
        .withColumn("description",   j("$.description"))
        .withColumn("physical_plan", j("$.physicalPlanDescription"))
        .withColumn("start_time_ms", j("$.time").cast("long"))
        .select("cluster_id", "app_id", "execution_id",
                "description", "physical_plan", "start_time_ms", "ingested_at")
    )

    ends = (
        dlt.read("bronze_raw_events")
        .filter(F.col("event_type") ==
                "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
        .withColumn("execution_id", j("$.executionId").cast("long"))
        .withColumn("end_time_ms",  j("$.time").cast("long"))
        .select("cluster_id", "app_id", "execution_id", "end_time_ms")
    )

    return starts.join(ends, on=["cluster_id", "app_id", "execution_id"], how="left")


# ------------------------------------------------------------
# G6. SQL Photon Analysis  ─  クエリ別 Photon 利用率
# ------------------------------------------------------------
@dlt.table(
    name="gold_sql_photon_analysis",
    comment="SQL実行ごとの Photon オペレーター利用率。Physical Plan の (N) OperatorName から集計。",
    table_properties={"quality": "gold"},
)
def gold_sql_photon_analysis():
    return (
        dlt.read("silver_sql_executions")
        # 全オペレーター名リスト: "(N) OperatorName" パターンを抽出
        .withColumn("all_operators",
            F.expr(r"regexp_extract_all(physical_plan, '\\((\\d+)\\)\\s+(\\w+)', 2)"))
        .withColumn("total_operators",  F.size(F.col("all_operators")))
        .withColumn("photon_operators",
            F.expr("size(filter(all_operators, x -> x like 'Photon%'))"))
        .withColumn("photon_pct",
            F.when(F.col("total_operators") > 0,
                F.round(F.col("photon_operators") / F.col("total_operators") * 100, 1)
            ).otherwise(None))
        .withColumn("is_photon",    F.col("photon_operators") > 0)
        .withColumn("duration_ms",  F.col("end_time_ms") - F.col("start_time_ms"))
        .withColumn("duration_sec", F.round(F.col("duration_ms") / 1000.0, 2))
        .withColumn("start_ts",     F.to_timestamp(F.col("start_time_ms") / 1000))
        .withColumn("description_short",
            F.regexp_replace(F.trim(F.col("description")), r"\s+", " "))
        # ─── ジョイン種別カウント ────────────────────────────────────────
        # BroadcastHashJoin (Spark非Photon) / PhotonBroadcastHashJoin (Photon)
        .withColumn("bhj_count",
            F.expr("size(filter(all_operators, x -> x = 'BroadcastHashJoin'))"))
        .withColumn("photon_bhj_count",
            F.expr("size(filter(all_operators, x -> x = 'PhotonBroadcastHashJoin'))"))
        .withColumn("smj_count",
            F.expr("size(filter(all_operators, x -> x = 'SortMergeJoin'))"))
        .withColumn("total_join_count",
            F.col("bhj_count") + F.col("photon_bhj_count") + F.col("smj_count"))
        # ─── 非Photon演算子リスト ────────────────────────────────────────
        # Photon未使用の原因特定用 (重複排除・カンマ区切り)
        .withColumn("non_photon_op_list",
            F.array_join(
                F.expr("array_distinct(filter(all_operators, x -> x NOT like 'Photon%'))"),
                ", "
            ))
        .select(
            "cluster_id", "app_id", "execution_id",
            "description_short", "start_ts", "duration_sec",
            "total_operators", "photon_operators", "photon_pct",
            F.col("is_photon").cast("string").alias("is_photon"),
            # ジョイン種別
            "bhj_count", "photon_bhj_count", "smj_count", "total_join_count",
            # 非Photon演算子
            "non_photon_op_list",
        )
    )
