# Databricks notebook source
# MAGIC %run "/Workspace/Users/qianhui_wang030@outlook.com/yelp/source/step2"

# COMMAND ----------

business_df.createOrReplaceTempView("business")
categories_df.createOrReplaceTempView("categories")
checkIn_df.createOrReplaceTempView("checkIn")

# COMMAND ----------

# MAGIC %md
# MAGIC ####How many reviews are there for each business?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT business_id, review_count
# MAGIC FROM business

# COMMAND ----------

# MAGIC %md
# MAGIC ####How many business take place in each state?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT state, count(*) AS count
# MAGIC FROM business
# MAGIC GROUP BY state

# COMMAND ----------

# MAGIC %md
# MAGIC ####How many business take place in each city?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, count(*) AS count
# MAGIC FROM business
# MAGIC GROUP BY city

# COMMAND ----------

# MAGIC %md
# MAGIC
