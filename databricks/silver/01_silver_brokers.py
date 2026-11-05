# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Silver: brokers
# MAGIC Bronze (Parquet from Azure SQL) -> clean and type -> data quality -> Silver Delta table.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("batch", "initial", "Bronze load folder")
batch = dbutils.widgets.get("batch")
TABLE = "brokers"
src = f"{BRONZE}/sql/{TABLE}/{batch}"
print("Reading:", src)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read Bronze

# COMMAND ----------

raw = bronze_meta(spark.read.format("parquet").load(src))
raw.printSchema()
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: clean and set data types
# MAGIC Trim spaces, fix capitalisation, cast every column to its proper type.

# COMMAND ----------

typed = raw.select(
    F.upper(F.trim("broker_id")).alias("broker_id"),
    F.initcap(F.trim("broker_name")).alias("broker_name"),
    F.initcap(F.trim("region")).alias("region"),
    F.col("commission_rate").cast("decimal(5,3)").alias("commission_rate"),
    F.col("hire_date").cast("date").alias("hire_date"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: data quality rules, quarantine, MERGE into Silver

# COMMAND ----------

reject_rules = {
    "missing_broker_id": "broker_id IS NULL OR broker_id = ''",
    "commission_out_of_range": "commission_rate < 0 OR commission_rate > 0.5",
}
data_cols = ["broker_id", "broker_name", "region", "commission_rate", "hire_date"]

finish_table(TABLE, batch, typed, data_cols, reject_rules,
             keys=["broker_id"], order_col="_bronze_ts")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: check the result

# COMMAND ----------

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
print("Silver rows:", out.count())
display(out.limit(10))
