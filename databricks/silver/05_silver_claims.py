# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Silver: claims
# MAGIC Source: CSV files in Bronze. The same claim can arrive several times with a new status
# MAGIC (Pending -> Approved). MERGE keeps one row per claim: the latest.
# MAGIC Needs Silver `policies` first.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("file_pattern", "claims_initial*.csv", "Bronze file name or pattern")
pattern = dbutils.widgets.get("file_pattern")
TABLE = "claims"
src = f"{BRONZE}/claims/{pattern}"
print("Reading:", src)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read the CSV
# MAGIC CSV has no types, so every column arrives as text. We read only the columns we know by name,
# MAGIC so a new extra column (schema drift, such as `adjuster_notes`) cannot break the notebook.

# COMMAND ----------

raw = bronze_meta(spark.read.option("header", True).csv(src))
raw.printSchema()
print("Bronze rows:", raw.count())
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: clean and type (text to dates, decimals, timestamps)

# COMMAND ----------

typed = raw.select(
    F.upper(F.trim("claim_id")).alias("claim_id"),
    F.upper(F.trim("policy_id")).alias("policy_id"),
    F.to_date("claim_date").alias("claim_date"),
    F.to_date("settlement_date").alias("settlement_date"),
    F.initcap(F.trim("claim_reason")).alias("claim_reason"),
    F.col("claim_amount").cast("decimal(12,2)").alias("claim_amount"),
    F.col("approved_amount").cast("decimal(12,2)").alias("approved_amount"),
    F.initcap(F.trim("claim_status")).alias("claim_status"),
    F.to_timestamp("last_modified").alias("last_modified"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: check the parent policy
# MAGIC Three cases: policy is in Silver (fine), policy was rejected into Quarantine (parent_policy_quarantined),
# MAGIC or the policy never existed anywhere (orphan_policy).

# COMMAND ----------

typed = (typed
         .join(silver_ids("policies", "policy_id"), "policy_id", "left")
         .join(quarantined_ids("policies", "policy_id"), "policy_id", "left"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: rules, quarantine, MERGE

# COMMAND ----------

reject_rules = {
    "missing_claim_id": "claim_id IS NULL OR claim_id = ''",
    "non_positive_claim_amount": "claim_amount <= 0",
    "approved_exceeds_claimed": "approved_amount > claim_amount",
    "settled_before_filed": "settlement_date < claim_date",
    "orphan_policy": "_in_silver IS NULL AND _in_quarantine IS NULL",
    "parent_policy_quarantined": "_in_silver IS NULL AND _in_quarantine IS TRUE",
}
flag_rules = {
    "unknown_status": "claim_status NOT IN ('Approved','Rejected','Pending','Under Review')",
    "decision_without_settlement_date": "claim_status IN ('Approved','Rejected') AND settlement_date IS NULL",
}
data_cols = ["claim_id", "policy_id", "claim_date", "settlement_date", "claim_reason",
             "claim_amount", "approved_amount", "claim_status", "last_modified"]

finish_table(TABLE, pattern, typed, data_cols, reject_rules,
             keys=["claim_id"], order_col="last_modified",
             flag_rules=flag_rules, drop_cols=["_in_silver", "_in_quarantine"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: check the result

# COMMAND ----------

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
display(out.groupBy("claim_status").count())
display(spark.read.format("delta").load(f"{QUAR}/{TABLE}").groupBy("reasons").count())
