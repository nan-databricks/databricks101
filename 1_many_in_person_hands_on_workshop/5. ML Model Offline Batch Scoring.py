# Databricks notebook source
# MAGIC %md 
# MAGIC ### We will now use the model to make a prediction on our historical silver table

# COMMAND ----------

dbutils.widgets.text("name","<<pls use same value as previous notebook>>") # enter your name, please use the same value used in the previous notebook

# COMMAND ----------

name = dbutils.widgets.get('name')
spark.sql(f'USE CATALOG {name}')
spark.sql(f'USE SCHEMA demo')

# COMMAND ----------

# MAGIC %md
# MAGIC # Get model uri from the results of Auto-ML
# MAGIC 1. Pick the best performing model in your Auto-ML results
# MAGIC 1. Click into the experiment
# MAGIC 1. Scroll to the bottom to 'Artifacts' and look for 'Full Path'
# MAGIC 1. Copy the path and paste it into 'model_uri path below'

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

import mlflow

model_uri = 'dbfs:/databricks/mlflow-tracking/3819540237154840/3e399103c6a546f883df8fbbd3440cf2/artifacts/model'

model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri)

# COMMAND ----------

model_features = model.metadata.get_input_schema().input_names()
print(model_features)

# COMMAND ----------

feature_df = spark.sql('select * from customer_churn_silver').drop('churn')

# COMMAND ----------

# MAGIC %md
# MAGIC # Run prediction using ML model

# COMMAND ----------

predictions = feature_df.withColumn('churnPredictions', model(*model_features)[0])
display(predictions.select("customerId", "churnPredictions"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Persist predicted churn table to a gold table 
# MAGIC ### Gold table will be used to build a churn prediction dashboard

# COMMAND ----------

gold_table_name = 'customer_churn_predictions'

# Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {gold_table_name}")

# COMMAND ----------

predictions.write.format('delta') \
                .mode('overwrite') \
                .saveAsTable(f'{gold_table_name}')

# COMMAND ----------

gold_df = spark.sql(f"SELECT * FROM {gold_table_name}")
display(gold_df)

# COMMAND ----------


