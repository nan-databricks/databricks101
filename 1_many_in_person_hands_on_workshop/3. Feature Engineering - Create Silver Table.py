# Databricks notebook source
# MAGIC %md
# MAGIC ### We will be doing some simple feature engineering here

# COMMAND ----------

dbutils.widgets.text("name","<<pls use same value as previous notebook>>") # enter your name, please use the same value used in the previous notebook

# COMMAND ----------

name = dbutils.widgets.get('name')
spark.sql(f'USE CATALOG {name}')
spark.sql(f'USE SCHEMA demo')
print(name)

# COMMAND ----------

bronze_df = spark.sql('select * from customer_churn_bronze')

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

import pyspark.pandas as ps

def compute_churn_features(data):
  
    # Convert to koalas
    data = data.to_pandas_on_spark()
    
    # 1-hot encoding of categorical columns
    data = ps.get_dummies(data, 
                        columns=['gender', 'Partner','MultipleLines',
                                 'Dependents','PhoneService',
                                 'InternetService', 'OnlineSecurity',
                                 'OnlineBackup', 'DeviceProtection', 'TechSupport',
                                 'StreamingTV', 'StreamingMovies', 'Contract',
                                 'PaperlessBilling','PaymentMethod'],dtype = 'int64')
    
    # Convert label to int and rename column
    data['Churn'] = data['Churn'].map({'Yes': 1, 'No': 0})
    data = data.astype({'Churn': 'int32'})
    data = data.rename(columns = {'Churn': 'churn'})

    # Clean up column names
    data.columns = data.columns.str.replace(' ', '')
    data.columns = data.columns.str.replace('(', '-')
    data.columns = data.columns.str.replace(')', '')

    # Drop missing values
    data = data.dropna()

    return data

# COMMAND ----------

churn_features_df = compute_churn_features(bronze_df)
display(churn_features_df)

# COMMAND ----------

silver_df = churn_features_df.to_spark()

# COMMAND ----------

silver_table_name = 'customer_churn_silver'

# Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {silver_table_name}")

# Clear path
# dbutils.fs.rm(f'{db_prefix_path}/silver',True)

# COMMAND ----------

silver_df.write.format('delta') \
                .mode('overwrite') \
                .saveAsTable(f'demo.{silver_table_name}')

# COMMAND ----------


