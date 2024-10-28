# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog capstone_project_1

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlssonydatabricks/raw/project1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers ;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze.customers FROM 'dbfs:/mnt/adlssonydatabricks/raw/project1/customers' FILEFORMAT = json COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products ;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze.products
# MAGIC  FROM 'dbfs:/mnt/adlssonydatabricks/raw/project1/products'
# MAGIC  FILEFORMAT = json COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.products

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.sales; 

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze.sales
# MAGIC FROM 'dbfs:/mnt/adlssonydatabricks/raw/project1/sales'
# MAGIC  FILEFORMAT = csv
# MAGIC  FORMAT_OPTIONS ('header'='true','mergeSchema' = 'true','inferSchema'='true')
# MAGIC  COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.sales

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists bronze.order_dates; 

# COMMAND ----------

# MAGIC %sql
# MAGIC  COPY INTO bronze.order_dates
# MAGIC  FROM 'dbfs:/mnt/adlssonydatabricks/raw/project1/order_dates'
# MAGIC  FILEFORMAT = csv
# MAGIC  FORMAT_OPTIONS ('header'='true','mergeSchema' = 'true','inferSchema'='true')
# MAGIC  COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.order_dates
