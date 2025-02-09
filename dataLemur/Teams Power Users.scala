// Databricks notebook source
/*
Write a query to identify the top 2 Power Users who sent the highest number of messages on Microsoft Teams in August 2022. Display the IDs of these 2 users along with the total number of messages they sent. Output the results in descending order based on the count of the messages.

Assumption:

No two users have sent the same number of messages in August 2022.
messages Table:
Column Name	Type
message_id	integer
sender_id	integer
receiver_id	integer
content	varchar
sent_date	datetime
messages Example Input:
+----------+---------+-----------+--------------------+-------------------+
|message_id|sender_id|receiver_id|             content|          sent_date|
+----------+---------+-----------+--------------------+-------------------+
|       901|     3601|       4500|             You up?|2022-08-03 00:00:00|
|       902|     4500|       3601|Only if you're bu...|2022-08-03 00:00:00|
|       743|     3601|       8752|Let's take this o...|2022-06-14 00:00:00|
|       922|     3601|       4500|     Get on the call|2022-08-10 00:00:00|
+----------+---------+-----------+--------------------+-------------------+
Example Output:
+---------+-------------+
|sender_id|message_count|
+---------+-------------+
|3601     |2            |
|4500     |1            |
+---------+-------------+

*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Define schema for Messages table
val messages_schema = StructType(Seq(
  StructField("message_id", IntegerType, nullable = false),
  StructField("sender_id", IntegerType, nullable = false),
  StructField("receiver_id", IntegerType, nullable = false),
  StructField("content", StringType, nullable = false),
  StructField("sent_date", StringType, nullable = false)
))

// Sample data
val messages_data = Seq(
  Row(901, 3601, 4500, "You up?", "2022-08-03 00:00:00"),
  Row(902, 4500, 3601, "Only if you're buying", "2022-08-03 00:00:00"),
  Row(743, 3601, 8752, "Let's take this offline", "2022-06-14 00:00:00"),
  Row(922, 3601, 4500, "Get on the call", "2022-08-10 00:00:00")
)

// Create DataFrame
val messages_df = spark.createDataFrame(spark.sparkContext.parallelize(messages_data), messages_schema)
  .withColumn("sent_date", col("sent_date").cast("timestamp")) // Ensure TimestampType

// Show DataFrame
messages_df.show()


// COMMAND ----------

val aug_df = messages_df.where("sent_date<'2022-09-01' and sent_date>'2022-07-31'")
aug_df.show(10,false)

// COMMAND ----------

val agg_df = aug_df.groupBy("sender_id").agg(count(col("content")).as("message_count"))
.orderBy(col("message_count").desc).limit(2)
agg_df.show(10,false)
