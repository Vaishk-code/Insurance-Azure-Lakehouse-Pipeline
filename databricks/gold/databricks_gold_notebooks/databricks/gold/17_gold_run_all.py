# Databricks notebook source
# MAGIC %md
# MAGIC # 17 Run all Gold notebooks in order
# MAGIC Order matters: dimensions first, then facts, then the checks.
# MAGIC Gold is rebuilt from Silver each time, so running this again is safe.

# COMMAND ----------

steps = [
    "./11_gold_dim_date",
    "./12_gold_dim_broker_policy",
    "./13_gold_dim_customer_scd2",
    "./14_gold_fact_claims",
    "./15_gold_fact_invoices",
    "./16_gold_fact_payments",
    "./18_gold_checks",
]

for notebook in steps:
    print("Running", notebook)
    dbutils.notebook.run(notebook, 3600)
    print("  done")
