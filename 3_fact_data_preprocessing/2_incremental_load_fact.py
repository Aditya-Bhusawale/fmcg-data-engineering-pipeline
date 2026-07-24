
# Cleaned incremental ETL notebook template

from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Load utilities
# %run /Workspace/Users/<your-user>/consolidated_pipelines/1_setup/utilities

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-ad75/{data_source}"
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# ---------------- BRONZE ----------------

df = (
    spark.read.options(header=True, inferSchema=True)
    .csv(f"{landing_path}/*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

df.write.format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("append")\
    .saveAsTable(bronze_table)

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

for file_info in dbutils.fs.ls(landing_path):
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# ---------------- SILVER ----------------

df_orders = spark.table(f"{catalog}.{bronze_schema}.staging_{data_source}")

df_orders = (
    df_orders
    .filter(col("order_qty").isNotNull())
    .withColumn(
        "customer_id",
        when(
            col("customer_id").rlike("^[0-9]+$"),
            col("customer_id")
        ).otherwise("999999").cast("string")
    )
    .withColumn(
        "order_placement_date",
        regexp_replace(
            "order_placement_date",
            r"^[A-Za-z]+,\s*",
            ""
        )
    )
    .withColumn(
        "order_placement_date",
        coalesce(
            try_to_date("order_placement_date","yyyy/MM/dd"),
            try_to_date("order_placement_date","dd-MM-yyyy"),
            try_to_date("order_placement_date","dd/MM/yyyy"),
            try_to_date("order_placement_date","MMMM dd, yyyy")
        )
    )
    .dropDuplicates([
        "order_id",
        "order_placement_date",
        "customer_id",
        "product_id",
        "order_qty"
    ])
    .withColumn("product_id",col("product_id").cast("string"))
)

df_products = spark.table(f"{catalog}.{silver_schema}.products")

df_joined = (
    df_orders
    .join(df_products,"product_id","inner")
    .select(df_orders["*"],df_products["product_code"])
    .dropDuplicates([
        "order_placement_date",
        "order_id",
        "product_code",
        "customer_id"
    ])
)

if not spark.catalog.tableExists(silver_table):

    df_joined.write.format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .option("mergeSchema","true")\
        .mode("overwrite")\
        .saveAsTable(silver_table)

else:

    DeltaTable.forName(spark,silver_table)\
        .alias("silver")\
        .merge(
            df_joined.alias("bronze"),
            """
            silver.order_placement_date = bronze.order_placement_date
            AND silver.order_id = bronze.order_id
            AND silver.product_code = bronze.product_code
            AND silver.customer_id = bronze.customer_id
            """
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

df_joined.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# ---------------- GOLD ----------------

df_gold = spark.sql(f"""
SELECT
    order_id,
    order_placement_date AS date,
    customer_id AS customer_code,
    product_code,
    product_id,
    order_qty AS sold_quantity
FROM {catalog}.{silver_schema}.staging_{data_source}
""")

if not spark.catalog.tableExists(gold_table):

    df_gold.write.format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .option("mergeSchema","true")\
        .mode("overwrite")\
        .saveAsTable(gold_table)

else:

    DeltaTable.forName(spark,gold_table)\
        .alias("target")\
        .merge(
            df_gold.alias("source"),
            """
            target.date = source.date
            AND target.order_id = source.order_id
            AND target.product_code = source.product_code
            AND target.customer_code = source.customer_code
            """
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# -------- Parent Gold Monthly Merge --------

incremental_dates = (
    spark.table(f"{catalog}.{silver_schema}.staging_{data_source}")
    .select(trunc("order_placement_date","MM").alias("start_month"))
    .distinct()
)

monthly_table = (
    spark.table(f"{catalog}.{gold_schema}.sb_fact_orders")
    .join(
        incremental_dates,
        trunc(col("date"),"MM")==col("start_month"),
        "inner"
    )
    .select("date","product_code","customer_code","sold_quantity")
)

df_monthly = (
    monthly_table
    .withColumn("month_start",trunc("date","MM"))
    .groupBy("month_start","product_code","customer_code")
    .agg(sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start","date")
)

DeltaTable.forName(
    spark,
    f"{catalog}.{gold_schema}.fact_orders"
).alias("parent")\
 .merge(
    df_monthly.alias("child"),
    """
    parent.date = child.date
    AND parent.product_code = child.product_code
    AND parent.customer_code = child.customer_code
    """
 )\
 .whenMatchedUpdateAll()\
 .whenNotMatchedInsertAll()\
 .execute()

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{bronze_schema}.staging_{data_source}")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{silver_schema}.staging_{data_source}")
