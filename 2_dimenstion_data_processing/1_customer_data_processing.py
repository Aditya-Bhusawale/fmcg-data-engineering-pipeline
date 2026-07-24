# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/aditya.bhusawale23@pccoepune.org/consolidated_pipelines/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg")
dbutils.widgets.text("data_source", "customers")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
print(catalog,data_source)


# COMMAND ----------

base_path=f's3://sportsbar-ad75/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df=spark.read.option("header","true").option("inferschema","true").csv(base_path).withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")
df.display()

# COMMAND ----------

df.printSchema()  

# COMMAND ----------

df.write.format('delta').option('delta.enableChangeDataFeed','true').mode('overwrite').saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver processing

# COMMAND ----------

df_bronze=spark.read.table(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

df_bronze.display()


# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_bronze.groupBy('customer_id').agg(count('customer_id').alias('cnt')).filter('cnt > 1').display()

# COMMAND ----------

print("rows before duplicate dropped:", df_bronze.count())
df_silver=df_bronze.dropDuplicates(['customer_id'])
print("rows after duplicate dropped:", df_silver.count())

# COMMAND ----------

display(df_silver.filter(col("customer_name")!=trim(col("customer_name"))))

# COMMAND ----------

df_silver=df_silver.withColumn('customer_name',trim(col('customer_name')))

# COMMAND ----------

display(df_silver.filter(col("customer_name")!=trim(col("customer_name"))))

# COMMAND ----------

# typos -> correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

df_silver=df_silver.replace(city_mapping,subset=['city'])

# COMMAND ----------

df_silver.select(col('city')).distinct().display()

# COMMAND ----------

df_silver=df_silver.withColumn('city',when(col('city').isNull(),'None').otherwise(col('city')))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_silver.select(col('city')).distinct().display()

# COMMAND ----------

df_silver=df_silver.withColumn('customer_name',initcap(col('customer_name')))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# Business Confirmation Note: City correction
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

data = list(customer_city_fix.items())

# Create DataFrame
df_fix = spark.createDataFrame(data, ["customer_id", "correct_city"])

df_fix.show()

# COMMAND ----------

df_silver=df_silver.join(df_fix,on='customer_id',how='left').withColumn('city',when(col('city')=='None',col('correct_city')).otherwise(col('city'))).drop('correct_city')


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver=df_silver.withColumn('customer_id',col('customer_id').cast('string'))

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver=df_silver.withColumn('customer',concat_ws('-',col('customer_name'),col('city'))).withColumn('market',lit('India')).withColumn('platform',lit('Sports Bar')).withColumn('channel',lit('Acquisition'))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold processing

# COMMAND ----------

df_silver=spark.read.table(f"{catalog}.{silver_schema}.{data_source}")

df_gold=df_silver.select('customer_id','customer_name','city','customer','market','platform','channel')

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
delta_child_table=spark.read.table("fmcg.gold.sb_dim_customers").select(col('customer_id').alias('customer_code'),col('customer'),col('market'),col('platform'),col('channel'))


# COMMAND ----------

delta_table.alias("target").merge(
    source=delta_child_table.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

