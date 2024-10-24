-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This excerise will be done in the 'SQL' tab (DBSQL)
-- MAGIC ### Some sample queries will be provided here for reference

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("name","<<pls use same value as previous notebook>>") # enter your name, please use the same value used in the previous notebook

-- COMMAND ----------

-- make sure to use the database 'name' used in previous notebook
USE CATALOG $name;
USE SCHEMA demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1. How many customers are at risk of churn?

-- COMMAND ----------

SELECT count(*) FROM customer_churn_predictions WHERE churnPredictions=1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/1_at_risk_customers.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2. How much monthly revenue is at risk from customers churning?

-- COMMAND ----------

SELECT SUM(monthlyCharges) FROM customer_churn_predictions WHERE churnPredictions=1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/2_mrr_at_risk.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3. What is the total monthly recurring revenue (MRR)?

-- COMMAND ----------

SELECT sum(monthlyCharges) FROM customer_churn_predictions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/3_total_mrr.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4. For the customers who will churn, how many have device protection?

-- COMMAND ----------

SELECT f.deviceProtection, count(*) as Customers
FROM customer_churn_predictions p
JOIN customer_churn_bronze f ON p.customerID = f.customerID
WHERE p.churnPredictions=1
GROUP BY f.deviceProtection

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/4_customer_churn_by_device_protection.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5. What is the breakdown of customer churn by tenure?

-- COMMAND ----------

SELECT tenure, churnPredictions, count(*) as customers
FROM customer_churn_predictions
GROUP BY tenure, churnPredictions


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/5_customer_churn_by_tenure.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6. For each customer that is predicted to churn, what is their monthly spend?

-- COMMAND ----------

SELECT f.customerID, f.monthlyCharges
FROM customer_churn_predictions p
JOIN customer_churn_bronze f ON p.customerID = f.customerID
WHERE p.churnPredictions=1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/6_customers_predicted_to_churn.png?raw=true" width=1000/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 7. What is the breakdown of customer churn per payment method?

-- COMMAND ----------

SELECT f.paymentMethod,p.churnPredictions, count(*) as Customers
FROM customer_churn_predictions p
JOIN customer_churn_bronze f ON p.customerID = f.customerID
GROUP BY f.paymentMethod, p.churnPredictions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Visualization setting : <br><br>
-- MAGIC <img src="https://github.com/deedeeck/databricks_assets/blob/main/pics/1_many_workshop/7_customer_churn_by_payment_method.png?raw=true" width=1000/>

-- COMMAND ----------


