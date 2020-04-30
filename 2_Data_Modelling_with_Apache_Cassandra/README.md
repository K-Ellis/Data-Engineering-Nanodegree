# NoSQL Data Modelling and ETL with Python and Apache Cassandra

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

To address this need, `Python` and `NoSQL` have been used to Extract, Transform and Load (ETL) the data into a `Apache Cassandra` database with tables designed to optimize queries on song play analysis. 

# Running the code
Use the following docker command to create the Apache Cassandra noSQL database
```bash
docker-compose up
```

Then run the Cassandra_NoSQL_ETL_Pipeline-checkpoint.ipynb notebook to Extract, Transform and Load the event data into the database.