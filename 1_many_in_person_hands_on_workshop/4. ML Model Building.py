# Databricks notebook source
# MAGIC %md
# MAGIC ### Now that we have a feature table, we can start building a ML model
# MAGIC ### We will be using Databricks AutoML to build a baseline ML model
# MAGIC
# MAGIC * This section will be done in the 'Machine Learning' tab
# MAGIC * We will kickstart Auto-ML first and while it is running, I will explain what Auto-ML does
# MAGIC * This exercise will be mostly done on the Auto-ML screen

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set timeout settings in Auto-ML to be 5 mins
# MAGIC * "Advanced configuration" -> "Stopping Conditions" -> "Timeout (minutes)" -> 5 mins

# COMMAND ----------

# MAGIC %md
# MAGIC # Let's talk about Auto-ML while it is doing training
# MAGIC <br> 
# MAGIC
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/automl1.jpg?raw=true"/> 
# MAGIC <br>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/automl2.jpg?raw=true"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/automl3.jpg?raw=true"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/automl4.jpg?raw=true"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/automl5.jpg?raw=true"/> 

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering
# MAGIC dbutils.library.restartPython()
# MAGIC
# MAGIC # %pip install databricks-sdk==0.17.0
# MAGIC # dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("name","<<pls use same value as previous notebook>>") # enter your name, please use the same value used in the previous notebook

# COMMAND ----------

name = dbutils.widgets.get('name')
spark.sql(f'USE CATALOG {name}')
spark.sql(f'USE SCHEMA demo')
print(name)

# COMMAND ----------

silver_df = spark.sql('select * from customer_churn_silver')

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

try:
  # Drop table if exists
  fe.drop_table('churn_user_features')
except:
  pass

# Create the feature table
churn_feature_table = fe.create_table(
  name=f'churn_user_features',
  primary_keys='customerID',
  schema=silver_df.schema,
  description='These features are derived from the customer_churn_silver table in the lakehouse. We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not. No aggregations were performed.'
)

# Write to the feature table
fe.write_table(df=silver_df, name='churn_user_features', mode='merge')

# Read from the feature table and display
features = fe.read_table(name='churn_user_features')
display(features)


# COMMAND ----------

from databricks import automl

automl_run = automl.classify(
    dataset = features,
    target_col = "churn",
    timeout_minutes = 10
)

# COMMAND ----------


