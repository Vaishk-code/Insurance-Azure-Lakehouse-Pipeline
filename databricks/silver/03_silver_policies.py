# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver: policies
# MAGIC Needs Silver `customers` and `brokers` first (run 01 and 02 before this).

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("batch", "initial", "Bronze load folder")
batch = dbutils.widgets.get("batch")
TABLE = "policies"
src = f"{BRONZE}/sql/{TABLE}/{batch}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read Bronze

# COMMAND ----------

raw = bronze_meta(spark.read.format("parquet").load(src))
print("Bronze rows:", raw.count())
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: clean and type

# COMMAND ----------

typed = raw.select(
    F.upper(F.trim("policy_id")).alias("policy_id"),
    F.upper(F.trim("customer_id")).alias("customer_id"),
    F.upper(F.trim("broker_id")).alias("broker_id"),
    F.initcap(F.trim("policy_type")).alias("policy_type"),
    F.col("coverage_amount").cast("decimal(12,2)").alias("coverage_amount"),
    F.col("annual_premium").cast("decimal(10,2)").alias("annual_premium"),
    F.col("start_date").cast("date").alias("start_date"),
    F.col("end_date").cast("date").alias("end_date"),
    F.initcap(F.trim("status")).alias("status"),
    F.col("last_modified").cast("timestamp").alias("last_modified"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: check parents exist (customer and broker)
# MAGIC We left-join to the Silver customer and broker keys. If there is no match, the helper column is NULL.

# COMMAND ----------

cust = silver_ids("customers", "customer_id").withColumnRenamed("_in_silver", "_cust_ok")
brok = silver_ids("brokers", "broker_id").withColumnRenamed("_in_silver", "_brok_ok")
typed = typed.join(cust, "customer_id", "left").join(brok, "broker_id", "left")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: rules, quarantine, MERGE

# COMMAND ----------

reject_rules = {
    "missing_policy_id": "policy_id IS NULL OR policy_id = ''",
    "end_before_start": "end_date < start_date",
    "non_positive_premium": "annual_premium <= 0",
    "non_positive_coverage": "coverage_amount <= 0",
}
flag_rules = {
    "unknown_policy_type": "policy_type NOT IN ('Auto','Home','Health','Life')",
    "unknown_status": "status NOT IN ('Active','Expired','Cancelled','Renewed')",
    "unknown_customer": "_cust_ok IS NULL",
    "unknown_broker": "_brok_ok IS NULL",
}
data_cols = ["policy_id", "customer_id", "broker_id", "policy_type", "coverage_amount",
             "annual_premium", "start_date", "end_date", "status", "last_modified"]

finish_table(TABLE, batch, typed, data_cols, reject_rules,
             keys=["policy_id"], order_col="last_modified",
             flag_rules=flag_rules, drop_cols=["_cust_ok", "_brok_ok"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: look at what was rejected and why

# COMMAND ----------

q = spark.read.format("delta").load(f"{QUAR}/{TABLE}")
display(q.groupBy("reasons").count())
display(q.limit(5))
