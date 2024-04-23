# Databricks notebook source
# MAGIC %md
# MAGIC ###Read files in

# COMMAND ----------

#business
business=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_business-1.json")
#checkin
checkin=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_checkin-1.json")
#review
review_1=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_review1.json")
review_2=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_review2.json")
review_3=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_review3.json")
review_4=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_review4.json")
review_5=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_review5.json")
review=review_1.union(review_2).union(review_3).union(review_4).union(review_5)
#tip
tip=spark.read.option("header","true").json("/FileStore/tables/yelp_academic_dataset_tip-1.json")

# COMMAND ----------


