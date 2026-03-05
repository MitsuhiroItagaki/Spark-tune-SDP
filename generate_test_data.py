# ==============================================================================
# テスト用 Spark Event Log 生成スクリプト
# Databricks ノートブック (Python) として実行してください
#
# 生成するシナリオ:
#   Stage 0  : 正常 (OK)
#   Stage 1  : データスキュー (DATA_SKEW)
#   Stage 2  : Disk Spill (DISK_SPILL)
#   Stage 3  : GC 高負荷 (HIGH_GC)
#   Stage 4  : Heavy Shuffle (HEAVY_SHUFFLE)
#   Stage 5  : ステージ失敗 (STAGE_FAILURE)
#
# 出力先: pipeline.log_root / pipeline.cluster_id / eventlog / <app_id>
# ==============================================================================

import json, time, random
from pathlib import Path

# -----------------------------------------------------------------------
# 設定  ← ご自身の環境に合わせて変更してください
# -----------------------------------------------------------------------
LOG_ROOT   = "/Volumes/main/default/my_volume/cluster-logs"  # UC Volume or s3://...
CLUSTER_ID = "0123-456789-test"
APP_ID     = "application_1700000000_0001"
OUTPUT_PATH = f"{LOG_ROOT}/{CLUSTER_ID}/eventlog/{APP_ID}"

# -----------------------------------------------------------------------
# タイムライン (ミリ秒)
# -----------------------------------------------------------------------
T0 = int(time.time() * 1000)
def t(delta_ms): return T0 + delta_ms

# -----------------------------------------------------------------------
# イベント生成ヘルパー
# -----------------------------------------------------------------------
def accumulables(metrics: dict) -> list:
    return [
        {"ID": i+1, "Name": k, "Value": str(v),
         "Internal": True, "Count Failed Values": False}
        for i, (k, v) in enumerate(metrics.items())
    ]

def stage_metrics(
    executor_run_ms, gc_ratio=0.05,
    disk_spill=0, mem_spill=0,
    shuffle_read=100*1024**2, shuffle_write=100*1024**2,
    input_bytes=1024**3, input_records=10_000_000,
):
    gc_ms = int(executor_run_ms * gc_ratio)
    return accumulables({
        "internal.metrics.executorRunTime":                executor_run_ms,
        "internal.metrics.executorCpuTime":                executor_run_ms * 900_000,
        "internal.metrics.jvmGarbageCollectionTime":       gc_ms,
        "internal.metrics.executorDeserializeTime":        500,
        "internal.metrics.resultSerializationTime":        200,
        "internal.metrics.resultSize":                     1024 * 100,
        "internal.metrics.peakExecutionMemory":            512 * 1024**2,
        "internal.metrics.memoryBytesSpilled":             mem_spill,
        "internal.metrics.diskBytesSpilled":               disk_spill,
        "internal.metrics.input.bytesRead":                input_bytes,
        "internal.metrics.input.recordsRead":              input_records,
        "internal.metrics.output.bytesWritten":            0,
        "internal.metrics.output.recordsWritten":          0,
        "internal.metrics.shuffle.read.remoteBytesRead":   int(shuffle_read * 0.3),
        "internal.metrics.shuffle.read.localBytesRead":    int(shuffle_read * 0.7),
        "internal.metrics.shuffle.read.recordsRead":       1_000_000,
        "internal.metrics.shuffle.read.fetchWaitTime":     100,
        "internal.metrics.shuffle.read.remoteBlocksFetched": 10,
        "internal.metrics.shuffle.read.localBlocksFetched":  20,
        "internal.metrics.shuffle.write.bytesWritten":     shuffle_write,
        "internal.metrics.shuffle.write.recordsWritten":   1_000_000,
        "internal.metrics.shuffle.write.writeTime":        1_000_000_000,
    })

def task_end(task_id, stage_id, executor_id, host, launch_ms, duration_ms,
             gc_ratio=0.05, spill_disk=0, spill_mem=0,
             shuffle_read=5*1024**2, shuffle_write=2*1024**2,
             succeeded=True):
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": 0,
        "Task Type": "ResultTask",
        "Task End Reason": {"Reason": "Success" if succeeded else "ExceptionFailure"},
        "Task Info": {
            "Task ID": task_id, "Index": task_id % 50, "Attempt": 0,
            "Launch Time": launch_ms, "Executor ID": str(executor_id),
            "Host": host, "Locality": "NODE_LOCAL",
            "Speculative": False, "Getting Result Time": 0,
            "Finish Time": launch_ms + duration_ms,
        },
        "Task Metrics": {
            "Executor Deserialize Time": 10, "Executor Deserialize CPU Time": 5_000_000,
            "Executor Run Time":        int(duration_ms * 0.9),
            "Executor CPU Time":        int(duration_ms * 0.9) * 900_000,
            "Peak Execution Memory":    64 * 1024**2,
            "Result Size":              1024,
            "JVM GC Time":              int(duration_ms * gc_ratio),
            "Result Serialization Time": 5,
            "Memory Bytes Spilled":     spill_mem,
            "Disk Bytes Spilled":       spill_disk,
            "Shuffle Read Metrics": {
                "Remote Blocks Fetched": 2, "Local Blocks Fetched": 8,
                "Fetch Wait Time": 5,
                "Remote Bytes Read":      int(shuffle_read * 0.3),
                "Remote Bytes Read To Disk": 0,
                "Local Bytes Read":       int(shuffle_read * 0.7),
                "Total Records Read":     10_000,
            },
            "Shuffle Write Metrics": {
                "Shuffle Bytes Written":   shuffle_write,
                "Shuffle Write Time":      10_000_000,
                "Shuffle Records Written": 10_000,
            },
            "Input Metrics":  {"Bytes Read": 10*1024**2, "Records Read": 100_000},
            "Output Metrics": {"Bytes Written": 0, "Records Written": 0},
        },
    }

# -----------------------------------------------------------------------
# イベントログ全体を組み立て
# -----------------------------------------------------------------------
events = []

# --- Application Start ---
events.append({
    "Event": "SparkListenerApplicationStart",
    "App Name": "TestSparkApp", "App ID": APP_ID,
    "Timestamp": t(0), "Spark User": "test@example.com", "Start Time": t(0),
})

# --- Environment Update ---
events.append({
    "Event": "SparkListenerEnvironmentUpdate",
    "JVM Information": {}, "Hadoop Properties": [], "System Properties": [],
    "Metrics Properties": {},
    "Spark Properties": [
        ["spark.sql.adaptive.enabled",            "false"],   # 意図的に無効
        ["spark.sql.adaptive.skewJoin.enabled",   "false"],
        ["spark.executor.memory",                 "8g"],
        ["spark.executor.cores",                  "4"],
        ["spark.sql.shuffle.partitions",          "200"],
        ["spark.databricks.delta.autoCompact.enabled", "true"],
    ],
})

# --- Executor Added (2台) ---
for eid, host in [(1, "10.0.0.1"), (2, "10.0.0.2")]:
    events.append({
        "Event": "SparkListenerExecutorAdded",
        "Timestamp": t(500),
        "Executor ID": str(eid),
        "Executor Info": {
            "Host": host, "Total Cores": 4,
            "Log Urls": {}, "Attributes": {}, "Resources": {},
            "Resource Profile Id": 0,
        },
    })

# -----------------------------------------------------------------------
# Job 0 : Stage 0 (正常) + Stage 1 (DATA_SKEW)
# -----------------------------------------------------------------------
events.append({
    "Event": "SparkListenerJobStart", "Job ID": 0,
    "Submission Time": t(1_000), "Stage IDs": [0, 1], "Properties": {},
})

# Stage 0: 正常 (OK)
NUM_TASKS = 50
STAGE_START = t(1_200)
for i in range(NUM_TASKS):
    events.append(task_end(
        task_id=i, stage_id=0,
        executor_id=(i % 2) + 1, host=f"10.0.0.{(i%2)+1}",
        launch_ms=STAGE_START + i * 20, duration_ms=1_000,
        gc_ratio=0.04,
    ))
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 0, "Stage Attempt ID": 0,
        "Stage Name": "count at pipeline.py:100",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + NUM_TASKS * 20 + 1_000,
        "Failure Reason": "",
        "Accumulables": stage_metrics(executor_run_ms=NUM_TASKS * 1_000),
    },
})

# Stage 1: DATA_SKEW (1タスクだけ10倍遅い)
STAGE_START = t(65_000)
for i in range(NUM_TASKS):
    dur = 50_000 if i == 0 else 1_000   # task 0 が極端に遅い
    events.append(task_end(
        task_id=NUM_TASKS + i, stage_id=1,
        executor_id=1, host="10.0.0.1",
        launch_ms=STAGE_START + i * 20, duration_ms=dur,
        gc_ratio=0.04,
    ))
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 1, "Stage Attempt ID": 0,
        "Stage Name": "join at pipeline.py:200 (DATA_SKEW)",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + 51_000,
        "Failure Reason": "",
        "Accumulables": stage_metrics(executor_run_ms=NUM_TASKS * 1_000),
    },
})
events.append({
    "Event": "SparkListenerJobEnd", "Job ID": 0,
    "Completion Time": t(120_000), "Job Result": {"Result": "JobSucceeded"},
})

# -----------------------------------------------------------------------
# Job 1 : Stage 2 (DISK_SPILL) + Stage 3 (HIGH_GC)
# -----------------------------------------------------------------------
events.append({
    "Event": "SparkListenerJobStart", "Job ID": 1,
    "Submission Time": t(120_500), "Stage IDs": [2, 3], "Properties": {},
})

# Stage 2: DISK_SPILL
STAGE_START = t(121_000)
for i in range(NUM_TASKS):
    events.append(task_end(
        task_id=NUM_TASKS*2 + i, stage_id=2,
        executor_id=(i % 2) + 1, host=f"10.0.0.{(i%2)+1}",
        launch_ms=STAGE_START + i * 50, duration_ms=3_000,
        gc_ratio=0.05, spill_disk=200*1024**2, spill_mem=50*1024**2,
    ))
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 2, "Stage Attempt ID": 0,
        "Stage Name": "sortBy at pipeline.py:300 (DISK_SPILL)",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + NUM_TASKS * 50 + 3_000,
        "Failure Reason": "",
        "Accumulables": stage_metrics(
            executor_run_ms=NUM_TASKS * 3_000,
            disk_spill=10 * 1024**3,    # 10GB disk spill
            mem_spill=2 * 1024**3,
        ),
    },
})

# Stage 3: HIGH_GC (GC オーバーヘッド 25%)
STAGE_START = t(280_000)
for i in range(NUM_TASKS):
    events.append(task_end(
        task_id=NUM_TASKS*3 + i, stage_id=3,
        executor_id=(i % 2) + 1, host=f"10.0.0.{(i%2)+1}",
        launch_ms=STAGE_START + i * 30, duration_ms=2_000,
        gc_ratio=0.28,   # 28% GC → HIGH_GC
    ))
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 3, "Stage Attempt ID": 0,
        "Stage Name": "aggregate at pipeline.py:400 (HIGH_GC)",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + NUM_TASKS * 30 + 2_000,
        "Failure Reason": "",
        "Accumulables": stage_metrics(
            executor_run_ms=NUM_TASKS * 2_000, gc_ratio=0.28,
        ),
    },
})
events.append({
    "Event": "SparkListenerJobEnd", "Job ID": 1,
    "Completion Time": t(400_000), "Job Result": {"Result": "JobSucceeded"},
})

# -----------------------------------------------------------------------
# Job 2 : Stage 4 (HEAVY_SHUFFLE) + Stage 5 (STAGE_FAILURE)
# -----------------------------------------------------------------------
events.append({
    "Event": "SparkListenerJobStart", "Job ID": 2,
    "Submission Time": t(400_500), "Stage IDs": [4, 5], "Properties": {},
})

# Stage 4: HEAVY_SHUFFLE (15GB shuffle read)
STAGE_START = t(401_000)
for i in range(NUM_TASKS):
    events.append(task_end(
        task_id=NUM_TASKS*4 + i, stage_id=4,
        executor_id=(i % 2) + 1, host=f"10.0.0.{(i%2)+1}",
        launch_ms=STAGE_START + i * 40, duration_ms=4_000,
        shuffle_read=300*1024**2, shuffle_write=300*1024**2,
    ))
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 4, "Stage Attempt ID": 0,
        "Stage Name": "reduceByKey at pipeline.py:500 (HEAVY_SHUFFLE)",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + NUM_TASKS * 40 + 4_000,
        "Failure Reason": "",
        "Accumulables": stage_metrics(
            executor_run_ms=NUM_TASKS * 4_000,
            shuffle_read=15 * 1024**3,   # 15GB → HEAVY_SHUFFLE
            shuffle_write=15 * 1024**3,
        ),
    },
})

# Stage 5: STAGE_FAILURE
STAGE_START = t(605_000)
events.append({
    "Event": "SparkListenerStageCompleted",
    "Stage Info": {
        "Stage ID": 5, "Stage Attempt ID": 0,
        "Stage Name": "collect at pipeline.py:600 (FAILURE)",
        "Number of Tasks": NUM_TASKS,
        "Submission Time": STAGE_START,
        "First Task Launched Time": STAGE_START + 200,
        "Completion Time": STAGE_START + 5_000,
        "Failure Reason": "Job aborted due to stage failure: Task 0 in stage 5.0 failed 4 times; cancelled remaining tasks",
        "Accumulables": stage_metrics(executor_run_ms=5_000),
    },
})
events.append({
    "Event": "SparkListenerJobEnd", "Job ID": 2,
    "Completion Time": t(615_000), "Job Result": {"Result": "JobFailed"},
})

# --- Executor Removed ---
for eid in [1, 2]:
    events.append({
        "Event": "SparkListenerExecutorRemoved",
        "Timestamp": t(620_000),
        "Executor ID": str(eid),
        "Removed Reason": "Job finished",
    })

# --- Application End ---
events.append({
    "Event": "SparkListenerApplicationEnd",
    "Timestamp": t(620_500),
})

# -----------------------------------------------------------------------
# ファイルへ書き出し (1行1JSON)
# -----------------------------------------------------------------------
lines = [json.dumps(e) for e in events]
content = "\n".join(lines)

dbutils.fs.mkdirs(f"{LOG_ROOT}/{CLUSTER_ID}/eventlog/")
dbutils.fs.put(OUTPUT_PATH, content, overwrite=True)

print(f"✅ イベントログを書き出しました: {OUTPUT_PATH}")
print(f"   総イベント数 : {len(events)}")
print(f"   総タスク数   : {NUM_TASKS * 5}")
print()
print("含まれるシナリオ:")
print("  Stage 0: OK              (正常)")
print("  Stage 1: DATA_SKEW       (task[0] が 50倍遅い)")
print("  Stage 2: DISK_SPILL      (10GB disk spill)")
print("  Stage 3: HIGH_GC         (GC オーバーヘッド 28%)")
print("  Stage 4: HEAVY_SHUFFLE   (15GB shuffle read)")
print("  Stage 5: STAGE_FAILURE   (ステージ失敗)")
print()
print("次のステップ: DLT パイプラインを以下の設定で実行してください")
print(f"  pipeline.log_root   = {LOG_ROOT}")
print(f"  pipeline.cluster_id = {CLUSTER_ID}")
