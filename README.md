# Yelp data
ETL the yelp date
### Files needed
  - ._yelp_academic_dataset_business.json
  - ._yelp_academic_dataset_checkin.json
  - ._yelp_academic_dataset_review.json
  - ._yelp_academic_dataset_tip.json


###File Structure
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
```
git clone https://github.com/github_username/repo_name.git
```

### Sources
    https://stackoverflow.com/questions/71545966/pyspark-schema-structure-to-read-nested-data
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



  