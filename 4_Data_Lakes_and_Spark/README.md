# Data Lake Architecture and ETL with Python and pySpark

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

To address this need, `Python` and `pySpark` have been used to Extract, Transform and Load (ETL) the data from its raw JSON format stored in Amazon S3 into a transformed Parquet format in partitioned Amazon S3 buckets.

# Running the code

Extract the raw data from the Amazon S3 bucket, transform this into a processed format and load this data into the Amazon S3 data lake in Parquet format.
```bash
python etl.py
``` 

# Data Lake Schema
The Amazon S3 Data Lake is comprised of the following partitioned parquet tables:

### Songs table 
- partitioned by year and artist_id
```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```

### Artists table                                              
```
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: double (nullable = true)
 |-- artist_latitude: double (nullable = true)
```

### Users table                                           
```
root
 |-- userId: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```

### Time table 
- partitioned by year and month
```
root
 |-- timestamp: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- weekday: integer (nullable = true)
 |-- weekday_name: string (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: integer (nullable = true)
```

### Songplays table 
- partitioned by year and month
```root
 |-- title: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- start_time: timestamp (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- songplay_id: long (nullable = false)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```
