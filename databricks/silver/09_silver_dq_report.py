# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Silver: data quality report
# MAGIC Summarises what was rejected (Quarantine), what was flagged, and the audit log.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame

TABLES = ["brokers", "customers", "policies", "invoices",
          "claims", "payments", "fraud_scores", "claim_notes"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Part 1: rejected rows by table and rule

# COMMAND ----------

parts = []
for t in TABLES:
    p = f"{QUAR}/{t}"
    if DeltaTable.isDeltaTable(spark, p):
        parts.append(spark.read.format("delta").load(p).select(
            F.lit(t).alias("table_name"),
            F.lit("rejected").alias("kind"),
            F.explode(F.split("reasons", "; ")).alias("rule")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Part 2: flagged rows (kept in Silver) by table and rule

# COMMAND ----------

for t in TABLES:
    p = f"{SILVER}/{t}"
    if DeltaTable.isDeltaTable(spark, p):
        parts.append(spark.read.format("delta").load(p).filter("dq_flags IS NOT NULL").select(
            F.lit(t).alias("table_name"),
            F.lit("flagged").alias("kind"),
            F.explode(F.split("dq_flags", ", ")).alias("rule")))

summary = (reduce(DataFrame.unionByName, parts)
           .groupBy("table_name", "kind", "rule").count()
           .orderBy("table_name", "kind", F.desc("count")))
summary.write.format("delta").mode("overwrite").save(f"{REPORTS}/dq_summary")
display(summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Part 3: audit log (latest run per table)
# MAGIC `duplicates_removed = rows_read - rows_silver - rows_quarantined`

# COMMAND ----------

audit = spark.read.format("delta").load(AUDIT)
w = Window.partitionBy("table_name").orderBy(F.col("run_ts").desc())
latest = (audit.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
          .withColumn("duplicates_removed", F.col("rows_read") - F.col("rows_silver") - F.col("rows_quarantined")))
display(latest.orderBy("table_name"))
