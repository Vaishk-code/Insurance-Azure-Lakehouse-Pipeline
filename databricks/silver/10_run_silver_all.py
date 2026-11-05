# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Run the whole Silver layer
# MAGIC Runs notebooks 01 to 09 in the right order (parents before children).
# MAGIC Safe to run again: every step uses MERGE.
# MAGIC Later, Data Factory will call this notebook with the day's parameters.

# COMMAND ----------

dbutils.widgets.text("batch", "initial", "SQL / notes Bronze folder")
dbutils.widgets.text("claims_pattern", "claims_initial*.csv", "Claims file pattern")
dbutils.widgets.text("payments_pattern", "payments_initial*.csv", "Payments file pattern")
dbutils.widgets.text("fraud_pattern", "fraud_scores_initial*.json", "Fraud file pattern")

batch = dbutils.widgets.get("batch")

steps = [
    ("./01_silver_brokers",      {"batch": batch}),
    ("./02_silver_customers",    {"batch": batch}),
    ("./03_silver_policies",     {"batch": batch}),
    ("./04_silver_invoices",     {"batch": batch}),
    ("./05_silver_claims",       {"file_pattern": dbutils.widgets.get("claims_pattern")}),
    ("./06_silver_payments",     {"file_pattern": dbutils.widgets.get("payments_pattern")}),
    ("./07_silver_fraud_scores", {"file_pattern": dbutils.widgets.get("fraud_pattern")}),
    ("./08_silver_claim_notes",  {"batch": batch}),
    ("./09_silver_dq_report",    {}),
]

# COMMAND ----------

for notebook, params in steps:
    print("Running", notebook, params)
    dbutils.notebook.run(notebook, 3600, params)
    print("  done")
