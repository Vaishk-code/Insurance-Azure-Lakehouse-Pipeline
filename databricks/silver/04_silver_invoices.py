# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Silver: invoices
# MAGIC Needs Silver `policies` first.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("batch", "initial", "Bronze load folder")
batch = dbutils.widgets.get("batch")
TABLE = "invoices"
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
    F.upper(F.trim("invoice_id")).alias("invoice_id"),
    F.upper(F.trim("policy_id")).alias("policy_id"),
    F.upper(F.trim("customer_id")).alias("customer_id"),
    F.col("invoice_date").cast("date").alias("invoice_date"),
    F.col("due_date").cast("date").alias("due_date"),
    F.col("amount").cast("decimal(10,2)").alias("amount"),
    F.initcap(F.trim("status")).alias("status"),
    F.col("last_modified").cast("timestamp").alias("last_modified"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: check the policy exists

# COMMAND ----------

pol = silver_ids("policies", "policy_id").withColumnRenamed("_in_silver", "_pol_ok")
typed = typed.join(pol, "policy_id", "left")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: rules, quarantine, MERGE

# COMMAND ----------

reject_rules = {
    "missing_invoice_id": "invoice_id IS NULL OR invoice_id = ''",
    "due_before_invoice_date": "due_date < invoice_date",
    "non_positive_amount": "amount <= 0",
}
flag_rules = {
    "unknown_status": "status NOT IN ('Issued','Paid','Partially Paid','Overdue','Cancelled')",
    "policy_not_in_silver": "_pol_ok IS NULL",
}
data_cols = ["invoice_id", "policy_id", "customer_id", "invoice_date", "due_date",
             "amount", "status", "last_modified"]

finish_table(TABLE, batch, typed, data_cols, reject_rules,
             keys=["invoice_id"], order_col="last_modified",
             flag_rules=flag_rules, drop_cols=["_pol_ok"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: check the result

# COMMAND ----------

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
display(out.groupBy("status").count())
display(spark.read.format("delta").load(f"{QUAR}/{TABLE}").groupBy("reasons").count())
