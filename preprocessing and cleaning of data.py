# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog capstone_project_1

# COMMAND ----------

df_sales=spark.table("bronze.sales")
df_sales_silver=df_sales.dropna(subset=["order_id","customer_id","transaction_id","product_id"]).dropDuplicates()
df_sales_silver.write.mode("overwrite").saveAsTable("silver.silver_sales")

# COMMAND ----------

from pyspark.sql.functions import current_date, col
df_products = spark.table("bronze.products").withColumn("ingestion_date", current_date())
df_products_today = df_products.filter(col("ingestion_date") == current_date()).drop("ingestion_date")
df_products_today.write.mode("overwrite").saveAsTable("temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.silver_product (
# MAGIC     product_category STRING,
# MAGIC     product_id BIGINT,
# MAGIC     product_name STRING,
# MAGIC     product_price DOUBLE
# MAGIC );
# MAGIC
# MAGIC WITH RankedProducts AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY (SELECT NULL)) AS rn
# MAGIC     FROM temp
# MAGIC ),
# MAGIC MaxRankPerProduct AS (
# MAGIC     SELECT product_id, MAX(rn) AS max_rn
# MAGIC     FROM RankedProducts
# MAGIC     GROUP BY product_id
# MAGIC )
# MAGIC MERGE INTO silver.silver_product AS target
# MAGIC USING (
# MAGIC     SELECT rp.*
# MAGIC     FROM RankedProducts rp
# MAGIC     JOIN MaxRankPerProduct mrpp ON rp.product_id = mrpp.product_id AND rp.rn = mrpp.max_rn
# MAGIC ) AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.product_category = source.product_category,
# MAGIC         target.product_name = source.product_name,
# MAGIC         target.product_price = source.product_price
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         product_category,
# MAGIC         product_id,
# MAGIC         product_name,
# MAGIC         product_price
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.product_category,
# MAGIC         source.product_id,
# MAGIC         source.product_name,
# MAGIC         source.product_price
# MAGIC     );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.silver_product

# COMMAND ----------

df_customers=spark.table("bronze.customers")
df_customers_silver=df_customers.dropna(subset="customer_id").dropDuplicates()
df_customers_silver.display()

# COMMAND ----------

df_dates=spark.table("bronze.order_dates")
df_dates_silver=df_dates.dropDuplicates()
df_dates_silver.display()

# COMMAND ----------

df_customers_silver.write.mode("overwrite").saveAsTable("silver.silver_customer")

# COMMAND ----------

df_dates_silver.write.mode("overwrite").saveAsTable("silver.silver_order_dates")
