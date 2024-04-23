# Databricks notebook source
# MAGIC %md
# MAGIC ###Import

# COMMAND ----------

#import
from pyspark.sql.functions import split, explode, collect_list, avg, count, round, sum, to_timestamp, filter
from pyspark.sql.types import IntegerType


# COMMAND ----------

# MAGIC %md
# MAGIC ###import data from step1

# COMMAND ----------

# MAGIC %run "/Workspace/Users/qianhui_wang030@outlook.com/yelp/source/step1"

# COMMAND ----------

business_temp=business
checkIn_temp=checkin
review_temp=review
tip_temp=tip

# COMMAND ----------

def categories_function(business):
    categories = business.select("business_id", "categories").toDF("business_id", "categories")
    categories=categories.withColumn("categories",split(categories["categories"],","))
    categories_df=categories.select("business_id",explode("categories").alias("categories"))
    return categories_df


#explode the date in checkIn with repeate business_id
def checkIn_function(checkin):
    checkin_df=checkin.withColumn("date",split(checkin["date"],","))
    checkin_df=checkin_df.select("business_id",explode("date").alias("date"))
    # checkin_df = checkin_df.withColumn("date", to_timestamp(checkin_df["date"], "yyyy-MM-dd HH:mm:ss"))
    return checkin_df


def business_function(business, review, tip):
    priceRange_df = business.select(business.business_id,
                    business.attributes.RestaurantsPriceRange2,
                    ).toDF(
                        "business_id","RestaurantsPriceRange2"
                    )
    priceRange_df = priceRange_df.withColumnRenamed("RestaurantsPriceRange2", "priceRange")
    priceRange_df = priceRange_df.withColumnRenamed("business_id", "priceRange_business_id")

    #extract the row that show the business is still open and extract the usefull column
    business_df = business.filter(business['is_open'] == 1).drop("is_open").drop("address").drop("latitude").drop("longitude").drop("postal_code").drop("hours").drop("attributes").drop("categories")

    #join df: business_df += priceRange_df
    business_df = business_df.join(priceRange_df, business_df.business_id == priceRange_df.priceRange_business_id).drop("priceRange_business_id")

    #rearrange the dataframe
    business_df = business_df.select("business_id", "name", "priceRange", "city", "state")


    #drop corrupt record and extract the usefull column
    review_df = review.filter(review["_corrupt_record"].isNull()).drop("_corrupt_record").drop("user_id").drop("text").drop("review_id").drop("funny").drop("date").drop("cool")

    #deal with repeat business_id by using aggregate function
    review_df = review_df.groupBy("business_id").agg(round(avg("stars"), 2).alias("average_stars"), count("*").alias("count"))
    review_df = review_df.withColumnRenamed("average_stars","review_avgStars")
    review_df = review_df.withColumnRenamed("count","review_count")
    review_df = review_df.withColumnRenamed("business_id","review_business_id")

    #join df: business_df += tip_df
    business_df = business_df.join(review_df, business_df.business_id == review_df.review_business_id).drop("review_business_id")

    #extract the usefull column from tip
    tip_df=tip.drop("date").drop("text").drop("user_id")

    #deal with repeat row
    tip_df = tip_df.withColumn("compliment_count", tip_df["compliment_count"].cast("int"))
    tip_df = tip_df.groupBy("business_id").agg(sum("compliment_count").alias("total_compliments"))
    tip_df = tip_df.withColumnRenamed("business_id", "tip_business_id")

    #join df: business_df += tip_df
    business_df = business_df.join(tip_df, business_df.business_id == tip_df.tip_business_id).drop("tip_business_id")

    business_df = business_df.withColumn("priceRange", business_df["priceRange"].cast(IntegerType()))\
                            .withColumn("review_count", business_df["review_count"].cast(IntegerType()))\
                            .withColumn("total_compliments", business_df["total_compliments"].cast(IntegerType())) 
            
    return business_df


def main(business_temp, checkIn_temp, review_temp, tip_temp):
    categories_df = categories_function(business_temp)
    business_df = business_function(business_temp, review_temp, tip_temp)
    checkIn_df = checkIn_function(checkIn_temp)

    return categories_df,business_df,checkIn_df

categories_df,business_df,checkIn_df= main(business_temp, checkIn_temp, review_temp, tip_temp)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Save Data

# COMMAND ----------

categories_df.write.option("compression", "snappy").mode("overwrite").parquet("/Workspace/Users/qianhui_wang030@outlook.com/yelp/final_data")
business_df.write.option("compression", "snappy").mode("overwrite").parquet("/Workspace/Users/qianhui_wang030@outlook.com/yelp/final_data")
checkIn_df.write.option("compression", "snappy").mode("overwrite").parquet("/Workspace/Users/qianhui_wang030@outlook.com/yelp/final_data")

# COMMAND ----------

display(categories_df)

# COMMAND ----------

display(business_df)

# COMMAND ----------

display(checkIn_df)

# COMMAND ----------

# MAGIC %md
# MAGIC test: example problem

# COMMAND ----------

business_df.printSchema()
