# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Silver: fraud scores
# MAGIC Source: JSON from the (mock) fraud API. Semi-structured: Spark reads the fields into columns.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("file_pattern", "fraud_scores_initial*.json", "Bronze file name or pattern")
pattern = dbutils.widgets.get("file_pattern")
TABLE = "fraud_scores"
src = f"{BRONZE}/fraud/{pattern}"
print("Reading:", src)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read the JSON
# MAGIC The file is one JSON array spread over many lines, so we set `multiLine`.

# COMMAND ----------

raw = bronze_meta(spark.read.option("multiLine", True).json(src))
raw.printSchema()
print("Bronze rows:", raw.count())
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: clean and type

# COMMAND ----------

typed = raw.select(
    F.upper(F.trim("claim_id")).alias("claim_id"),
    F.col("fraud_score").cast("double").alias("fraud_score"),
    F.initcap(F.trim("risk_band")).alias("risk_band"),
    F.to_timestamp("scored_at").alias("scored_at"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: rules, quarantine, MERGE
# MAGIC The `risk_band_mismatch` flag checks that the band agrees with the score:
# MAGIC High at 0.6 or more, Medium at 0.3 or more, otherwise Low.

# COMMAND ----------

reject_rules = {
    "missing_claim_id": "claim_id IS NULL OR claim_id = ''",
    "score_out_of_range": "fraud_score < 0 OR fraud_score > 1",
}
flag_rules = {
    "risk_band_mismatch": ("risk_band <> CASE WHEN fraud_score >= 0.6 THEN 'High' "
                           "WHEN fraud_score >= 0.3 THEN 'Medium' ELSE 'Low' END"),
}
data_cols = ["claim_id", "fraud_score", "risk_band", "scored_at"]

finish_table(TABLE, pattern, typed, data_cols, reject_rules,
             keys=["claim_id"], order_col="scored_at", flag_rules=flag_rules)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: check the result

# COMMAND ----------

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
display(out.groupBy("risk_band").count())
