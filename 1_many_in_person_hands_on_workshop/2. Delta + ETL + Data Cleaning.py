# Databricks notebook source
dbutils.widgets.text("name","<<pls use same value as previous notebook>>") # enter your name, please use the same value used in the previous notebook

# COMMAND ----------

name = dbutils.widgets.get('name')
db_prefix_path = f"/Volumes/{name}/demo/customer_churn"
spark.sql(f'USE CATALOG {name}')
spark.sql(f'USE SCHEMA demo')
print(name)

# COMMAND ----------

# some stats about the bronze table
bronze_df = spark.sql('select * from customer_churn_bronze')
print(f"num rows: {bronze_df.count()}\n")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_churn_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Delta?
# MAGIC <br>
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/delta_overview.jpg?raw=true" width=1000/>
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Taking a peek into delta file format

# COMMAND ----------

# %md
# <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/delta_get_started.jpg?raw=true" width=1000/>

# COMMAND ----------

# Run the DESCRIBE DETAIL command
table_details = spark.sql(f"DESCRIBE DETAIL customer_churn_bronze")

# Display the details
display(table_details)

# COMMAND ----------

# Extract the storage location
storage_location_bronze = table_details.select("location").collect()[0]["location"]
print(f"Storage location: {storage_location_bronze}")

# COMMAND ----------

display(dbutils.fs.ls(storage_location_bronze))

# COMMAND ----------

display(dbutils.fs.ls(f'{storage_location_bronze}/_delta_log'))

# COMMAND ----------

# formatting json so it can be printed nicely
import json

jsl = dbutils.fs.head(f'{storage_location_bronze}/_delta_log/00000000000000000000.json',99999).split('\n')
jsl.pop()
[print(json.dumps(json.loads(x),indent=2)) for x in jsl]

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling Schemas: Enforcement and Evolution
# MAGIC Most of the time, we want strict schemas to be adhered to so that downstream teams and processes that rely on that schema don't break. But sometimes the data coming in is changing rapidly and we need to evolve our schemas instead of strictly enforcing them. Delta supports both [schema enforcement and evolution](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html). Let's dive into each

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by **rejecting writes to a table that do not match the table’s schema**. 

# COMMAND ----------

# Let's synthesis some bad data to show how delta handles schema enforcement
bad_schema_df = (
  spark.read.table("customer_churn_bronze")
  .sample(fraction=0.1)
  .withColumnRenamed("gender", "gender_wrong_column") # purposely breaking schema by changing col name
)
display(bad_schema_df)

# COMMAND ----------

try:
    bad_schema_df\
    .write\
    .mode("append")\
    .saveAsTable("customer_churn_bronze")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_churn_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution
# MAGIC Say you actually want the flexibility of ever-changing schemas. Add an extra line of code to allow for schema evolution!

# COMMAND ----------

(
  bad_schema_df
  .write
  .mode("append")
  .option("mergeSchema", "true") # <- 1 line to allow for schema evolution
  .saveAsTable("customer_churn_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_churn_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_churn_bronze where gender_wrong_column <> 'null'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_churn_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # Restore Delta Table to previous version
# MAGIC Oops! Shouldn't have merged that schema! Good thing it's super simple to roll back to a previous version of a Delta table!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We're at version 1 after our most recent append, so let's roll back to version 0
# MAGIC RESTORE TABLE customer_churn_bronze TO VERSION AS OF 0;
# MAGIC
# MAGIC DESCRIBE HISTORY customer_churn_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC # Time Travel
# MAGIC
# MAGIC Since we're keeping a history of changes over time, we can also travel back in time to a previous verion of our table! There's lots of different ways to explore your history as well. 
# MAGIC
# MAGIC For example, by timestamp:
# MAGIC
# MAGIC In Python:
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .option("timestampAsOf", "2019-01-01") \
# MAGIC   .load("/path/to/my/table")
# MAGIC   ```
# MAGIC SQL syntax:
# MAGIC ```sql
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF "2019-01-01"
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF date_sub(current_date(), 1)
# MAGIC SELECT count(*) FROM my_table TIMESTAMP AS OF "2019-01-01 01:30:00.000"
# MAGIC ```
# MAGIC
# MAGIC or by version number:
# MAGIC Python syntax:
# MAGIC ```python
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .option("versionAsOf", "5238") \
# MAGIC   .load("/path/to/my/table")
# MAGIC
# MAGIC df = spark.read \
# MAGIC   .format("delta") \
# MAGIC   .load("/path/to/my/table@v5238")
# MAGIC ```
# MAGIC SQL syntax:
# MAGIC ```sql
# MAGIC SELECT count(*) FROM my_table VERSION AS OF 5238
# MAGIC SELECT count(*) FROM my_table@v5238
# MAGIC SELECT count(*) FROM delta.`/path/to/my/table@v5238`
# MAGIC ```
# MAGIC
# MAGIC Let's try!

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history customer_churn_bronze

# COMMAND ----------

display(dbutils.fs.ls(f'{storage_location_bronze}/_delta_log'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_churn_bronze VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Handling change data via MERGE command
# MAGIC
# MAGIC ### Hypothetical scenario
# MAGIC #### Let's say there is an accounting error and all monthly charges below $30 have to be increased by $20
# MAGIC #### How do we update the data in place?

# COMMAND ----------

import pyspark.sql.functions as F

change_df = spark.sql('SELECT * FROM customer_churn_bronze')

change_df = change_df.filter('MonthlyCharges < 30').withColumn('MonthlyCharges', F.col('MonthlyCharges') + 20 )
change_df.createOrReplaceTempView('change_data')

display(change_df.select('customerID','MonthlyCharges'))

# COMMAND ----------

display(spark.sql('SELECT customerID,MonthlyCharges FROM customer_churn_bronze'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE UPSERT
# MAGIC Okay, so now we have an updated dataframe! But how should we update values in our delta table...? Why not with [MERGE UPSERT](https://docs.databricks.com/delta/delta-update.html#upsert-into-a-table-using-merge) syntax?!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- show count of silver tables to demonstrate how many rows will be updated
# MAGIC select count(*) from customer_churn_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now use MERGE INTO to update the historical table
# MAGIC MERGE INTO 
# MAGIC customer_churn_bronze AS orig 
# MAGIC USING change_data AS cd 
# MAGIC
# MAGIC ON orig.customerID = cd.customerID
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC
# MAGIC -- if this was an update table that also had new rows as well, then we could insert on NOT MATCHED like so:
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *;

# COMMAND ----------

display(spark.sql('SELECT customerID,MonthlyCharges FROM customer_churn_bronze where customerID = "7590-VHVEG"'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Only 1000+ rows were updated! The remaining 5000+ rows were not!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- let's roll back this merge to the original bronze table as the accounting error was a hypothetical scenario
# MAGIC RESTORE TABLE customer_churn_bronze TO VERSION AS OF 0;
# MAGIC
# MAGIC DESCRIBE HISTORY customer_churn_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Well that was kinda cool! I guess? Seems obvious. Well, you would think so, but merge upserts is actually non-trivial to implement.
# MAGIC
# MAGIC In this graphic, each box represents a parquet file
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2019/03/UpsertsBlog.jpg" alt="MERGE INTO" style="width: 1000px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # VACUUM
# MAGIC As you can imagine, keeping large table histories takes up more and more space in S3/ADLSg2/GCS. It is very important to VACUUM your Delta tables to delete history that is older than you desire to keep (and pay cloud storage fees for). This is done through the VACUUM command.
# MAGIC
# MAGIC **NOTE**: If you run VACUUM on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customer_churn_bronze

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

display(dbutils.fs.ls(storage_location_bronze))

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM customer_churn_bronze RETAIN 0 HOURS DRY RUN
# MAGIC -- dry run will return the files that vaccum command will remove
# MAGIC -- to execute the vaccum command for real, remove 'DRY RUN'

# COMMAND ----------

# MAGIC %md
# MAGIC # Recap
# MAGIC Alright, we've explored a ton of features above that make Delta both **reliable** and **performant**. 
# MAGIC
# MAGIC Let's break down what we covered so far: 
# MAGIC
# MAGIC **Exploring how the delta file format looks like**
# MAGIC - We look at the underlying _delta_logs folder
# MAGIC - We also looked at the underlying .json file that is created for each Delta version
# MAGIC - Each write operation to a delta table is considered a vesion
# MAGIC
# MAGIC **Schema Enforcement & Evolution**
# MAGIC - We tried adding in new data but discovered a Schema Mismatch thanks to Delta's Schema Enforcement feature
# MAGIC - We added the data to our table anyway to explore Delta's Schema Evolution feature
# MAGIC - We saw what happens when you try to merge in an incorrect schema
# MAGIC - We then allowed the schema to evolve with 1 extra line of code
# MAGIC - We RESTOREd our table to a previous, correct version
# MAGIC
# MAGIC **Time Travel**
# MAGIC - We then TIME TRAVELED and restored a previous version of our Delta table
# MAGIC
# MAGIC **MERGE UPSERT**
# MAGIC - Created an interpolation table for the faulty heart rate readings
# MAGIC - Used MERGE UPSERT syntax to merge the corrections into our table
# MAGIC     
# MAGIC **VACUUM**
# MAGIC - Delete table history older than a certain threshold

# COMMAND ----------


