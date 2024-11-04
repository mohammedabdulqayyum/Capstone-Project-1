# Databricks notebook source
# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE capstone_project_1.bronze.customers TO `user_email@example.com`;
# MAGIC GRANT INSERT ON TABLE capstone_project_1.bronze.sales TO `user_email@example.com`;
# MAGIC GRANT UPDATE ON TABLE capstone_project_1.bronze.products TO `user_email@example.com`;
# MAGIC GRANT DELETE ON TABLE capstone_project_1.bronze.order_dates TO `user_email@example.com`;
