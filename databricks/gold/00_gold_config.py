# 00_gold_config: small helpers for the Gold notebooks

GOLD = lake("gold")
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
