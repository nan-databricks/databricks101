-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks Introduction Notebook

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("name","<<pls fill this in>>")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fill in the text box at the top with your own name
-- MAGIC
-- MAGIC 1. Preferably a unique value that would not clash with other users
-- MAGIC 1. Please remember this name as we would be using it for the rest of the workshop
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Attach the notebook to the cluster and run all commands in the notebook
-- MAGIC
-- MAGIC 1. Return to this notebook. 
-- MAGIC 1. In the notebook menu bar, attach the cluster.
-- MAGIC 1. When the cluster changes from <img src="http://docs.databricks.com/_static/images/clusters/cluster-starting.png"/></a> to <img src="http://docs.databricks.com/_static/images/clusters/cluster-running.png"/></a>, click <img src="	https://docs.databricks.com/en/_images/nb-run-all.png"/></a> button.
-- MAGIC

-- COMMAND ----------

-- Create catalog for the schemas
CREATE CATALOG IF NOT EXISTS $name;

-- Set default catalog
USE CATALOG $name;

-- Delete the old schema and tables if needed
DROP SCHEMA IF EXISTS demo CASCADE;

-- Create schema for tables
CREATE SCHEMA IF NOT EXISTS demo;

-- Use schema
USE SCHEMA demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## The next command creates a table from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC name = dbutils.widgets.get('name')
-- MAGIC print(name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql('DROP TABLE IF EXISTS diamonds')
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").mode("overwrite").saveAsTable('diamonds')

-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results 
-- MAGIC
-- MAGIC Specifically, the command:
-- MAGIC 1. Selects color and price columns, averages the price, and groups and orders by color.
-- MAGIC 1. Displays a table of the results.

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Convert the table to a chart
-- MAGIC
-- MAGIC Under the table, click the bar chart + icon. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds_df = spark.sql('select * from diamonds')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC
-- MAGIC display(diamonds_df.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

-- COMMAND ----------


