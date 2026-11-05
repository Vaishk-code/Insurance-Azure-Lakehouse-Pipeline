# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Silver: payments
# MAGIC Each payment must point to an invoice. Needs Silver `invoices` first.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

dbutils.widgets.text("file_pattern", "payments_initial*.csv", "Bronze file name or pattern")
pattern = dbutils.widgets.get("file_pattern")
TABLE = "payments"
src = f"{BRONZE}/payments/{pattern}"
print("Reading:", src)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: read the CSV

# COMMAND ----------

raw = bronze_meta(spark.read.option("header", True).csv(src))
print("Bronze rows:", raw.count())
display(raw.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: clean and type

# COMMAND ----------

typed = raw.select(
    F.upper(F.trim("payment_id")).alias("payment_id"),
    F.upper(F.trim("invoice_id")).alias("invoice_id"),
    F.upper(F.trim("policy_id")).alias("policy_id"),
    F.to_date("due_date").alias("due_date"),
    F.to_date("paid_date").alias("paid_date"),
    F.col("amount").cast("decimal(10,2)").alias("amount"),
    F.trim("payment_method").alias("payment_method"),
    F.initcap(F.trim("status")).alias("status"),
    "_source_file", "_bronze_ts",
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: check the parent invoice

# COMMAND ----------

typed = (typed
         .join(silver_ids("invoices", "invoice_id"), "invoice_id", "left")
         .join(quarantined_ids("invoices", "invoice_id"), "invoice_id", "left"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: rules, quarantine, MERGE
# MAGIC Partial payments are kept and flagged: they are real business events, not errors.

# COMMAND ----------

reject_rules = {
    "missing_payment_id": "payment_id IS NULL OR payment_id = ''",
    "non_positive_amount": "amount <= 0",
    "orphan_invoice": "_in_silver IS NULL AND _in_quarantine IS NULL",
    "parent_invoice_quarantined": "_in_silver IS NULL AND _in_quarantine IS TRUE",
}
flag_rules = {
    "partial_payment": "status = 'Partial'",
    "paid_without_date": "status IN ('Paid','Late','Partial') AND paid_date IS NULL",
    "unknown_status": "status NOT IN ('Paid','Late','Partial','Missed')",
}
data_cols = ["payment_id", "invoice_id", "policy_id", "due_date", "paid_date",
             "amount", "payment_method", "status"]

finish_table(TABLE, pattern, typed, data_cols, reject_rules,
             keys=["payment_id"], order_col="_bronze_ts",
             flag_rules=flag_rules, drop_cols=["_in_silver", "_in_quarantine"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: check the result

# COMMAND ----------

out = spark.read.format("delta").load(f"{SILVER}/{TABLE}")
display(out.groupBy("status").count())
display(spark.read.format("delta").load(f"{QUAR}/{TABLE}").groupBy("reasons").count())
