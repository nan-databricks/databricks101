# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Loading data into Databricks

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Enter input value above

# COMMAND ----------

dbutils.widgets.text("name","<<pls fill this in>>") # enter your name, preferable something unique that wont clash with other users

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulling csv file from github

# COMMAND ----------

name = dbutils.widgets.get("name")
print(name)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Set default Unity Catalog
# MAGIC

# COMMAND ----------

# Create catalog for the schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {name}")

# Set default catalog
spark.sql(f"USE CATALOG {name}")

# Delete the old schema and tables if needed
spark.sql(f"DROP SCHEMA IF EXISTS demo CASCADE")

# Create schema for tables
spark.sql(f"CREATE SCHEMA IF NOT EXISTS demo")

# Use schema
spark.sql(f"USE SCHEMA demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading in Databricks

# COMMAND ----------

import urllib3
response = urllib3.PoolManager().request('GET', 'https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv')
csvfile = response.data.decode("utf-8")

# COMMAND ----------

# Create a managed volume
spark.sql(f"CREATE VOLUME IF NOT EXISTS customer_churn")

# Show all volumes
spark.sql(f"SHOW VOLUMES")

# COMMAND ----------

# /Volumes/<catalog>/<schema>/<volume_name>
db_prefix_path = f"/Volumes/{name}/demo/customer_churn"
db_raw_path = f"{db_prefix_path}/raw/Telco-Customer-Churn.csv"
dbutils.fs.put(db_raw_path, csvfile, True)

# COMMAND ----------

bronze_df = spark.read.format('csv').option('header','true').option("inferSchema","true").load(db_raw_path)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persisting data into a bronze table

# COMMAND ----------

bronze_table_name = "customer_churn_bronze"

# Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")

# Clear path
dbutils.fs.rm(f'{db_prefix_path}/bronze',True)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/delta_get_started.jpg?raw=true" width=1000/>

# COMMAND ----------

bronze_df.write.format('delta') \
                .mode('overwrite') \
                .saveAsTable(f'{bronze_table_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Medallion Architecture
# MAGIC <img src="https://www.databricks.com/sites/default/files/inline-images/building-data-pipelines-with-delta-lake-120823.png?v=1702318922" width=1000/>

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_churn_bronze

# COMMAND ----------


