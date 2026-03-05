# Databricks notebook source

# MAGIC %md
# MAGIC # Spark Performance — AI サマリー生成
# MAGIC
# MAGIC Gold テーブルのメトリクスを読み込み、選択したLLMモデルで分析テキストを生成して
# MAGIC ダッシュボードの「処理概要」「改善インパクト TOP3」テキストウィジェットを自動更新します。
# MAGIC
# MAGIC **実行タイミング:** DLT パイプライン完了後に Databricks Workflow からトリガーするか、手動で実行してください。

# COMMAND ----------

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURATION — 環境に合わせて変更してください                          │
# └─────────────────────────────────────────────────────────────────────────┘

# Gold テーブルが格納されているカタログ.スキーマ
SCHEMA = "main.base2"

# 生成履歴を保存するテーブル（自動作成されます）
HISTORY_TABLE = f"{SCHEMA}.gold_narrative_summary"

# ダッシュボード ID（URLの /dashboardsv3/<id>/published から取得）
DASHBOARD_ID = "01f116951e3d1ef5a108a3c246a2b84a"

# ──────────────────────────────────────────────────────────────────────────
# 使用するモデルの Databricks Model Serving エンドポイント名を指定してください。
#
# Databricks Hosted (Pay-per-token) の主要モデル:
#   "databricks-claude-opus-4-6"      ← Claude Opus 4.6    (最高精度・推奨)
#   "databricks-claude-sonnet-4-6"    ← Claude Sonnet 4.6  (精度とコストのバランス)
#   "databricks-gpt-5-2"              ← GPT-5.2
#   "databricks-meta-llama-3-3-70b-instruct"  ← Llama 3.3 70B (OSS)
#
# External Model エンドポイント（自前で登録した場合）:
#   登録したエンドポイント名をそのまま指定してください。
# ──────────────────────────────────────────────────────────────────────────
MODEL_ENDPOINT = "databricks-claude-opus-4-6"

# 出力言語 ("ja" = 日本語 / "en" = English)
OUTPUT_LANG = "ja"

# COMMAND ----------

# %pip install openai --quiet

# COMMAND ----------

import json
import requests
from datetime import datetime, timezone
from openai import OpenAI
from pyspark.sql import functions as F

# 認証情報を Databricks コンテキストから自動取得
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get().rstrip("/")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# OpenAI 互換クライアント（Databricks Model Serving）
client = OpenAI(
    api_key=token,
    base_url=f"{host}/serving-endpoints"
)

print(f"Model  : {MODEL_ENDPOINT}")
print(f"Schema : {SCHEMA}")
print(f"Host   : {host}")

# COMMAND ----------

# %md ## 1. Gold テーブルからメトリクスを収集

# COMMAND ----------

def df_to_dict_list(df, limit=10):
    return [row.asDict() for row in df.limit(limit).collect()]

# アプリケーションサマリー
app_summary = spark.sql(f"""
    SELECT
        total_jobs, successful_jobs, failed_jobs,
        ROUND(job_success_rate_pct, 1) AS job_success_rate_pct,
        ROUND(total_shuffle_read_gb, 2)  AS total_shuffle_read_gb,
        ROUND(total_shuffle_write_gb, 2) AS total_shuffle_write_gb,
        ROUND(total_disk_spill_gb, 2)    AS total_disk_spill_gb,
        ROUND(gc_overhead_pct, 1)        AS gc_overhead_pct,
        ROUND(app_duration_min, 1)       AS app_duration_min
    FROM {SCHEMA}.gold_application_summary
    LIMIT 1
""").collect()
app_row = app_summary[0].asDict() if app_summary else {}

# ボトルネック件数サマリー
bn_summary = spark.sql(f"""
    SELECT bottleneck_type, severity, COUNT(*) AS cnt
    FROM {SCHEMA}.gold_bottleneck_report
    GROUP BY bottleneck_type, severity
    ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, cnt DESC
""")
bn_rows = df_to_dict_list(bn_summary, 20)

# 遅いジョブ TOP5
slow_jobs = spark.sql(f"""
    SELECT job_id, ROUND(duration_sec, 1) AS duration_sec, job_result, stage_ids
    FROM {SCHEMA}.gold_job_performance
    ORDER BY duration_sec DESC NULLS LAST
    LIMIT 5
""")
slow_job_rows = df_to_dict_list(slow_jobs)

# 最悪ステージ TOP5（実行時間順）
worst_stages = spark.sql(f"""
    SELECT stage_id, stage_name, bottleneck_type, severity,
           ROUND(duration_ms/1000.0, 1) AS duration_sec,
           num_tasks,
           ROUND(task_skew_ratio, 1)    AS task_skew_ratio,
           ROUND(shuffle_read_mb, 0)    AS shuffle_read_mb,
           ROUND(gc_overhead_pct, 1)    AS gc_overhead_pct,
           recommendation
    FROM {SCHEMA}.gold_stage_performance
    WHERE bottleneck_type != 'OK'
    ORDER BY duration_ms DESC NULLS LAST
    LIMIT 5
""")
worst_stage_rows = df_to_dict_list(worst_stages)

# CPU・並列実行（効率が低いジョブ TOP5）
cpu_rows = spark.sql(f"""
    SELECT job_id,
           ROUND(duration_sec, 1)           AS duration_sec,
           concurrent_jobs_at_start,
           ROUND(job_cpu_efficiency_pct, 1) AS cpu_efficiency_pct,
           ROUND(total_gc_time_sec, 1)      AS gc_time_sec
    FROM {SCHEMA}.gold_job_concurrency
    ORDER BY job_cpu_efficiency_pct ASC NULLS LAST
    LIMIT 5
""")
cpu_job_rows = df_to_dict_list(cpu_rows)

# Photon 利用率（低いSQL TOP5）
photon_rows = spark.sql(f"""
    SELECT description_short,
           ROUND(duration_sec, 1) AS duration_sec,
           ROUND(photon_pct, 1)   AS photon_pct,
           non_photon_op_list
    FROM {SCHEMA}.gold_sql_photon_analysis
    WHERE duration_sec IS NOT NULL
    ORDER BY photon_pct ASC, duration_sec DESC NULLS LAST
    LIMIT 5
""")
photon_job_rows = df_to_dict_list(photon_rows)

print("Metrics collected.")
print(f"  App summary      : {app_row}")
print(f"  Bottlenecks      : {len(bn_rows)} types")
print(f"  Slow jobs        : {len(slow_job_rows)}")
print(f"  Worst stages     : {len(worst_stage_rows)}")
print(f"  Low CPU jobs     : {len(cpu_job_rows)}")
print(f"  Low Photon SQLs  : {len(photon_job_rows)}")

# COMMAND ----------

# %md ## 2. プロンプト構築

# COMMAND ----------

lang_instruction = (
    "すべての出力を日本語で記述してください。"
    if OUTPUT_LANG == "ja"
    else "Write all output in English."
)

system_prompt = f"""あなたは Apache Spark パフォーマンス分析の専門家です。
提供されたメトリクスデータをもとに、エンジニアが即座に行動できる具体的な分析テキストを生成してください。
{lang_instruction}
必ず以下のJSON形式のみで返答してください（JSON以外のテキストは不要）:
{{
  "summary_text": "<Markdown形式のテキスト>",
  "top3_text": "<Markdown形式のテキスト>"
}}"""

user_prompt = f"""以下はDatabricks Sparkジョブのパフォーマンス分析結果です。

=== アプリケーション全体サマリー ===
{json.dumps(app_row, ensure_ascii=False, indent=2)}

=== ボトルネック件数（種別・重要度別） ===
{json.dumps(bn_rows, ensure_ascii=False, indent=2)}

=== 実行時間が長いジョブ TOP5 ===
{json.dumps(slow_job_rows, ensure_ascii=False, indent=2)}

=== ボトルネックが深刻なステージ TOP5 ===
{json.dumps(worst_stage_rows, ensure_ascii=False, indent=2)}

=== CPU効率が低いジョブ TOP5 ===
{json.dumps(cpu_job_rows, ensure_ascii=False, indent=2)}

=== Photon利用率が低いSQL TOP5 ===
{json.dumps(photon_job_rows, ensure_ascii=False, indent=2)}

---
上記データをもとに以下2つのMarkdownテキストを生成してください。

【summary_text】処理概要と主なボトルネック（1000文字以内）
- 処理全体の傾向（ジョブ数・成功率・シャッフル量など）
- 検出されたボトルネックの種類・件数・重要度
- 最も重大な問題点の概要
- 箇条書きで簡潔に

【top3_text】改善インパクト TOP3 ジョブ（1500文字以内）
- 改善効果が最も大きいと思われるジョブを3つ選定
- 各ジョブについて: 問題の根本原因・具体的な改善施策・期待される効果
- job_id や数値（秒・MB等）を明示して具体的に記述
"""

print("Prompt built.")
print(f"  System prompt length : {len(system_prompt)} chars")
print(f"  User prompt length   : {len(user_prompt)} chars")

# COMMAND ----------

# %md ## 3. LLM 呼び出し

# COMMAND ----------

print(f"Calling {MODEL_ENDPOINT} ...")

response = client.chat.completions.create(
    model=MODEL_ENDPOINT,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ],
    max_tokens=4000,
    temperature=0.3,
)

raw_output = response.choices[0].message.content
print("LLM response received.")
print(f"  Tokens used: {response.usage.total_tokens}")
print(f"  Output preview: {raw_output[:200]}...")

# COMMAND ----------

# %md ## 4. レスポンスをパース

# COMMAND ----------

# JSON ブロックを抽出（```json ... ``` で囲まれている場合も考慮）
import re
json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
assert json_match, f"JSON not found in response:\n{raw_output}"

parsed = json.loads(json_match.group())
summary_text = parsed["summary_text"]
top3_text    = parsed["top3_text"]

print("=== summary_text ===")
print(summary_text)
print("\n=== top3_text ===")
print(top3_text)

# COMMAND ----------

# %md ## 5. 生成履歴を Delta テーブルに保存

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
        generated_at  TIMESTAMP,
        model_name    STRING,
        schema_name   STRING,
        summary_text  STRING,
        top3_text     STRING,
        prompt_tokens INT,
        total_tokens  INT
    )
    USING DELTA
""")

spark.createDataFrame([{
    "generated_at" : datetime.now(timezone.utc),
    "model_name"   : MODEL_ENDPOINT,
    "schema_name"  : SCHEMA,
    "summary_text" : summary_text,
    "top3_text"    : top3_text,
    "prompt_tokens": response.usage.prompt_tokens,
    "total_tokens" : response.usage.total_tokens,
}]).write.mode("append").saveAsTable(HISTORY_TABLE)

print(f"Saved to {HISTORY_TABLE}")

# COMMAND ----------

# %md ## 6. ダッシュボードのテキストウィジェットを更新

# COMMAND ----------

SUMMARY_WIDGET_NAME = "summary00"
TOP3_WIDGET_NAME    = "top3text0"

# 現在のダッシュボード定義を取得
r = requests.get(f"{host}/api/2.0/lakeview/dashboards/{DASHBOARD_ID}", headers=headers)
assert r.status_code == 200, f"GET dashboard failed: {r.status_code} {r.text}"

current = r.json()
serialized = json.loads(current["serialized_dashboard"])

# textbox_spec を名前で特定して更新
updated_summary = False
updated_top3    = False
for page in serialized["pages"]:
    for item in page["layout"]:
        widget = item["widget"]
        if widget.get("name") == SUMMARY_WIDGET_NAME:
            widget["textbox_spec"] = summary_text
            updated_summary = True
        elif widget.get("name") == TOP3_WIDGET_NAME:
            widget["textbox_spec"] = top3_text
            updated_top3 = True

assert updated_summary, f"Widget '{SUMMARY_WIDGET_NAME}' not found in dashboard"
assert updated_top3,    f"Widget '{TOP3_WIDGET_NAME}' not found in dashboard"

# PATCH でダッシュボードを更新（URLは変わらない）
patch_payload = {
    "display_name"         : current["display_name"],
    "serialized_dashboard" : json.dumps(serialized),
}
r = requests.patch(
    f"{host}/api/2.0/lakeview/dashboards/{DASHBOARD_ID}",
    headers=headers, json=patch_payload
)
assert r.status_code == 200, f"PATCH dashboard failed: {r.status_code} {r.text}"

# 公開
r = requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{DASHBOARD_ID}/published",
    headers=headers, json={"warehouse_id": "bec52b183a4cfe2a"}
)
assert r.status_code == 200, f"PUBLISH failed: {r.status_code} {r.text}"

print("Dashboard updated and published!")
print(f"URL: {host}/dashboardsv3/{DASHBOARD_ID}/published")
