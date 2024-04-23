# Yelp data
ETL the yelp date
### Files needed
The data list below for this project can be found in the "original_data" folder. This folder contains the raw data files in JSON format that were used for the ETL process. You can explore these files to understand the structure and content of the raw data before it was transformed and loaded into the Spark DataFrame.
  - ._yelp_academic_dataset_business.json
  - ._yelp_academic_dataset_checkin.json
  - ._yelp_academic_dataset_review.json
  - ._yelp_academic_dataset_tip.json
  

### File Structure
```
yelp
  final_data/
  source/
      - step1
      - step2
  tests/
      - step1_test
      - step2_test
  README.md
```

### Installation
Clone the repo
```
git clone https://github.com/QianhuiWang080/yelp.git
```

### Reference Sources
  - ##### PySpark Schema Structure to Read Nested Data
    This link provides useful information about reading nested data using PySpark
    
    https://stackoverflow.com/questions/71545966/pyspark-schema-structure-to-read-nested-data
    
  - ##### Databricks PySpark: Explode and Pivot Columns
    This article introduces the methods of aggregation groupBy and explode

    https://medium.com/@shuklaprashant9264/databricks-pyspark-explode-and-pivot-columns-a8d1b4e713f1#:~:text=The%20explode%20function%20in%20PySpark,of%20the%20original%20array%20column.&text=As%20you%20can%20see%2C%20the,numbers%20column%20into%20multiple%20rows.

### Outcome DataFrame
  - business_df
    - business_id (string)
    - name (string)
    - priceRange (integer)
    - city (string)
    - state (string)
    - review_avgStars  (double)
    - review_count (integer)
    - total_compliments (integer)

  - categories_df
    - business_id (string)
    - categories (string)

  - checkIn_df
    - business_id (string)
    - date (string)
### Contact
- email: qianhui_wang030@outlook.com
- Project Link: https://github.com/QianhuiWang080/yelp

### Acknowledgments
The dataframe "checkIn_df" has a column name date, but the schema of it is string, fail to change it to timestamp due to error below, becareful when using it.
"SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:"


  