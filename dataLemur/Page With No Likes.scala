// Databricks notebook source
/*
Assume you're given two tables containing data about Facebook Pages and their respective likes (as in "Like a Facebook Page").

Write a query to return the IDs of the Facebook pages that have zero likes. The output should be sorted in ascending order based on the page IDs.

pages Table:
Column Name	Type
page_id	integer
page_name	varchar
pages Example Input:
+-------+----------------------+
|page_id|page_name             |
+-------+----------------------+
|20001  |SQL Solutions         |
|20045  |Brain Exercises       |
|20701  |Tips for Data Analysts|
+-------+----------------------+

page_likes Table:
Column Name	Type
user_id	integer
page_id	integer
liked_date	datetime
page_likes Example Input:
+-------+-------+-------------------+
|user_id|page_id|         liked_date|
+-------+-------+-------------------+
|    111|  20001|2022-04-08 00:00:00|
|    121|  20045|2022-03-12 00:00:00|
|    156|  20001|2022-07-25 00:00:00|
+-------+-------+-------------------+
Example Output:
+-------+
|page_id|
+-------+
|20701  |
+-------+
 
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
val pages_schema = StructType(Seq(
  StructField("page_id", IntegerType, nullable = false),
  StructField("page_name", StringType, nullable = false)
))
 
val pages_data = Seq(
  Row(20001, "SQL Solutions"),
  Row(20045, "Brain Exercises"),
  Row(20701, "Tips for Data Analysts")
)
 
val pages_df = spark.createDataFrame(spark.sparkContext.parallelize(pages_data), pages_schema)
pages_df.show(10,false)

// COMMAND ----------

 
val likes_schema = StructType(Seq(
  StructField("user_id", IntegerType, nullable = false),
  StructField("page_id", IntegerType, nullable = false),
  StructField("liked_date", StringType, nullable = false)
))

// Sample data
val likes_data = Seq(
  Row(111, 20001, "2022-04-08 00:00:00"),
  Row(121, 20045, "2022-03-12 00:00:00"),
  Row(156, 20001, "2022-07-25 00:00:00")
)

// Create DataFrame
val likes_df = spark.createDataFrame(spark.sparkContext.parallelize(likes_data), likes_schema)
  .withColumn("liked_date",col("liked_date").cast("timestamp")) 

// Show DataFrame
likes_df.show()


// COMMAND ----------

val page_likes_df = pages_df.join(likes_df,Seq("page_id"),"left")
  .groupBy("page_id").agg(count(col("user_id")).as("like_count"))

// COMMAND ----------

val no_likes_df = page_likes_df.where("like_count=0").select("page_id").orderBy(col("page_id"))
no_likes_df.show(10,false)

// COMMAND ----------


