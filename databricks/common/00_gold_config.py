# Databricks notebook source
# MAGIC %md
# MAGIC # 00_gold_config: small helpers for the Gold notebooks
# MAGIC This notebook does three things:
# MAGIC 1. Loads your existing Silver settings (`../common/00_config`, where you already set your storage account name)
# MAGIC 2. Defines where Gold tables are saved
# MAGIC 3. Gives you 3 small helper functions: `load_silver`, `load_gold`, `save_gold`
# MAGIC
# MAGIC **You do not need to change anything here**, except `AS_OF` if you stopped at a different day.

# COMMAND ----------
# MAGIC %run ../common/00_config

# COMMAND ----------

# Gold tables are saved in the "gold" container
GOLD = lake("gold")

# "Today" for this project. Used for invoice aging and open-claim age.
# We loaded days up to 5 July 2025, so we pretend today is 5 July 2025.
# If you stopped at a different day, change this date.
AS_OF = "2025-07-05"


def load_silver(name):
    """Read a Silver table and give it a SQL name: silver_<name>.
    Example: load_silver("claims") lets you write: SELECT * FROM silver_claims"""
    df = spark.read.format("delta").load(f"{SILVER}/{name}")
    df.createOrReplaceTempView(f"silver_{name}")
    return df


def load_gold(name):
    """Read a Gold table and give it a SQL name (same as the table name).
    Example: load_gold("dim_customer") lets you write: SELECT * FROM dim_customer"""
    df = spark.read.format("delta").load(f"{GOLD}/{name}")
    df.createOrReplaceTempView(name)
    return df


def save_gold(df, name):
    """Save a DataFrame as a Gold table (replaces the old version, so re-running is safe)."""
    path = f"{GOLD}/{name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    n = spark.read.format("delta").load(path).count()
    print(f"Saved {name}: {n} rows")
