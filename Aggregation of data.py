# Databricks notebook source
df_cust2 = spark.table("capstone_project_1.silver.silver_customer")
df_order2 = spark.table("capstone_project_1.silver.silver_order_dates")
df_product2 = spark.table("capstone_project_1.silver.silver_product")
df_sales2 = spark.table("capstone_project_1.silver.silver_sales")

# COMMAND ----------

df_joined = df_sales2.join(
    df_cust2, "customer_id","inner"
).join(
    df_product2, "product_id","inner"
).join(
    df_order2, "order_date","inner"
)

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.createOrReplaceTempView("total_revenue")

# COMMAND ----------

total_revenue_df = spark.sql("select sum(total_amount) as total_revenue from total_revenue")
total_revenue_df.display()

# COMMAND ----------

total_revenue_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.total_revenue")

# COMMAND ----------

df_joined.createOrReplaceTempView("total_discount")

# COMMAND ----------

total_discount_df = spark.sql("select sum(discount_amount) as total_discount from total_discount")
total_discount_df.display()

# COMMAND ----------

total_discount_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.total_discount")

# COMMAND ----------

df_joined.createOrReplaceTempView("avg_order_value")

# COMMAND ----------

avg_order_value_df = spark.sql("select avg(total_amount) as average_order_value from avg_order_value")
avg_order_value_df.display()

# COMMAND ----------

avg_order_value_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.avg_order_value")

# COMMAND ----------

df_joined.createOrReplaceTempView("no_of_orders")

# COMMAND ----------

no_of_orders_df = spark.sql("select count(distinct order_id) as number_of_orders from no_of_orders")
no_of_orders_df.display()

# COMMAND ----------

no_of_orders_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.no_of_orders")

# COMMAND ----------

df_joined.createOrReplaceTempView("top5_products_by_sales")

# COMMAND ----------

top5_products_by_sales_df = spark.sql("select product_name, sum(total_amount) as total_sales from top5_products_by_sales group by product_name order by total_sales desc limit 5")
top5_products_by_sales_df.display()

# COMMAND ----------

top5_products_by_sales_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.top5_products_by_sales")

# COMMAND ----------

df_joined.createOrReplaceTempView("revenue_by_customer_loc")

# COMMAND ----------

revenue_by_customer_loc_df = spark.sql("select customer_city,customer_state, sum(total_amount) as total_revenue from revenue_by_customer_loc group by customer_city,customer_state order by total_revenue desc")
revenue_by_customer_loc_df.display()

# COMMAND ----------

revenue_by_customer_loc_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.revenue_by_customer_loc")

# COMMAND ----------

df_joined.createOrReplaceTempView("frequency")

# COMMAND ----------

frequency_df = spark.sql("select order_date,count(distinct order_id) as number_of_orders from frequency group by order_date order by order_date")
frequency_df.display()

# COMMAND ----------

frequency_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.frequency")

# COMMAND ----------

df_joined.createOrReplaceTempView("trends")

# COMMAND ----------

trends_df = spark.sql("select order_date, sum(total_amount) as total_revenue from trends group by order_date order by order_date")
trends_df.display()

# COMMAND ----------

trends_df.write.mode("overwrite").saveAsTable("capstone_project_1.gold.trends")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

masked_trends_df = trends_df.withColumn("total_revenue", 
                                        regexp_replace(col("total_revenue").cast("string"), ".", "*"))

masked_trends_df.display()


# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
masked_frequency_df = frequency_df.withColumn("number_of_orders", 
                                              regexp_replace(col("number_of_orders").cast("string"), ".", "*"))
display(masked_frequency_df)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

masked_top5products_df = top5_products_by_sales_df.withColumn("product_name", 
                                                    regexp_replace(col("product_name"), ".", "*")) \
                                        .withColumn("total_sales", 
                                                    regexp_replace(col("total_sales").cast("string"), ".", "*"))

# Display the masked DataFrame to verify the masking
display(masked_top5products_df)
