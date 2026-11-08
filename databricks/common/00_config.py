 # 00_config: shared settings and helper functions

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from datetime import datetime

STORAGE_ACCOUNT = "insurancelake"   


def lake(container, path=""):
    base = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base


BRONZE = lake("bronze")
SILVER = lake("silver")
QUAR = f"{SILVER}/_quarantine"          
AUDIT = f"{SILVER}/_audit/run_log"      
REPORTS = f"{SILVER}/_reports"          

# Helper 1: add file metadata (which Bronze file a row came from, and when it landed)

def bronze_meta(df):
    """Call right after spark.read. Adds _source_file and _bronze_ts."""
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_bronze_ts"),
    )

# Helper 2: data quality rules

def apply_rules(df, reject_rules, flag_rules=None):
    flag_rules = flag_rules or {}

    def build(rules):
        if not rules:
            return F.array().cast("array<string>")
        return F.array(*[F.when(F.expr(expr), F.lit(name)) for name, expr in rules.items()])

    df = df.withColumn("_reject", F.filter(build(reject_rules), lambda x: x.isNotNull()))
    df = df.withColumn("_flags", F.filter(build(flag_rules), lambda x: x.isNotNull()))
    bad = df.filter(F.size("_reject") > 0)
    good = df.filter(F.size("_reject") == 0).drop("_reject")
    return good, bad

# Helper 3: keep only the latest version of each key

def dedupe_latest(df, keys, order_col):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last(), F.col("_bronze_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# Helper 4: write rejected rows to Quarantine (safe to re-run: replaces only this batch)

def write_quarantine(bad, table, batch_id, data_cols):
    q = bad.select(
        F.to_json(F.struct(*[F.col(c) for c in data_cols])).alias("row_json"),
        F.concat_ws("; ", F.col("_reject")).alias("reasons"),
        F.lit(table).alias("source_table"),
        F.lit(batch_id).alias("batch_id"),
        F.current_timestamp().alias("quarantined_ts"),
    )
    path = f"{QUAR}/{table}"
    if DeltaTable.isDeltaTable(spark, path):
        (q.write.format("delta").mode("overwrite")
          .option("replaceWhere", f"batch_id = '{batch_id}'").save(path))
    else:
        q.write.format("delta").mode("overwrite").save(path)

# Helper 5: MERGE (upsert) into a Silver Delta table

def upsert_delta(df, path, keys, order_col):
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        return
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll(condition=f"s.{order_col} >= t.{order_col}")
        .whenNotMatchedInsertAll()
        .execute())

# Helper 6: referential checks (does the parent row exist?)

def silver_ids(table, key):
    """Distinct keys currently in a Silver table."""
    return (spark.read.format("delta").load(f"{SILVER}/{table}")
            .select(key).distinct().withColumn("_in_silver", F.lit(True)))


def quarantined_ids(table, key):
    """Distinct keys of rows rejected from a table (read back from its quarantine JSON)."""
    path = f"{QUAR}/{table}"
    if DeltaTable.isDeltaTable(spark, path):
        return (spark.read.format("delta").load(path)
                .select(F.get_json_object("row_json", f"$.{key}").alias(key))
                .distinct().withColumn("_in_quarantine", F.lit(True)))
    return spark.createDataFrame([], f"{key} string, _in_quarantine boolean")

# Helper 7: audit log (one row per run: rows read / written / rejected)

def log_run(table, batch_id, rows_read, rows_silver, rows_quarantined):
    row = [(datetime.utcnow().isoformat(), table, batch_id, rows_read, rows_silver, rows_quarantined)]
    schema = ("run_ts string, table_name string, batch_id string, "
              "rows_read long, rows_silver long, rows_quarantined long")
    spark.createDataFrame(row, schema).write.format("delta").mode("append").save(AUDIT)

# Helper 8: the standard finishing steps used by every table

def finish_table(table, batch, df, data_cols, reject_rules, keys, order_col,
                 flag_rules=None, drop_cols=None, history_keys=None):
    drop_cols = drop_cols or []
    df = df.cache()
    n_read = df.count()

    good, bad = apply_rules(df, reject_rules, flag_rules)
    good = (good.withColumn("dq_flags",
                            F.when(F.size("_flags") > 0, F.concat_ws(", ", F.col("_flags"))))
                .drop("_flags", *drop_cols))

    n_bad = bad.count()
    write_quarantine(bad, table, batch, data_cols)

    if history_keys:   # keep every version of the row (used later for SCD Type 2)
        hist = (dedupe_latest(good, history_keys, "_bronze_ts")
                .withColumn("_silver_ts", F.current_timestamp()))
        upsert_delta(hist, f"{SILVER}/{table}_history", history_keys, "_bronze_ts")

    current = (dedupe_latest(good, keys, order_col)
               .withColumn("_silver_ts", F.current_timestamp()))
    n_silver = current.count()
    upsert_delta(current, f"{SILVER}/{table}", keys, order_col)

    log_run(table, batch, n_read, n_silver, n_bad)
    df.unpersist()
    print(f"{table}: read={n_read}  to_silver={n_silver}  quarantined={n_bad}  "
          f"duplicates_removed={n_read - n_silver - n_bad}")
    return n_read, n_silver, n_bad
