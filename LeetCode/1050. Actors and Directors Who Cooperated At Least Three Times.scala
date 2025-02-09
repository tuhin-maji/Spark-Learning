// Databricks notebook source
/*
Table: ActorDirector

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| actor_id    | int     |
| director_id | int     |
| timestamp   | int     |
+-------------+---------+
timestamp is the primary key (column with unique values) for this table.
 

Write a solution to find all the pairs (actor_id, director_id) where the actor has cooperated with the director at least three times.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
ActorDirector table:
+-------------+-------------+-------------+
| actor_id    | director_id | timestamp   |
+-------------+-------------+-------------+
| 1           | 1           | 0           |
| 1           | 1           | 1           |
| 1           | 1           | 2           |
| 1           | 2           | 3           |
| 1           | 2           | 4           |
| 2           | 1           | 5           |
| 2           | 1           | 6           |
+-------------+-------------+-------------+
Output: 
+-------------+-------------+
| actor_id    | director_id |
+-------------+-------------+
| 1           | 1           |
+-------------+-------------+
Explanation: The only pair is (1, 1) where they cooperated exactly 3 times.
 
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
 
val schema = StructType(Seq(
  StructField("actor_id", IntegerType, nullable = false),
  StructField("director_id", IntegerType, nullable = false),
  StructField("timestamp", IntegerType, nullable = false)
))

// Create the data as a sequence
val data = Seq(
  (1, 1, 0),
  (1, 1, 1),
  (1, 1, 2),
  (1, 2, 3),
  (1, 2, 4),
  (2, 1, 5),
  (2, 1, 6)
)

// Convert sequence to DataFrame
val df = spark.createDataFrame(data).toDF("actor_id", "director_id", "timestamp")

// Show the DataFrame
df.show()


// COMMAND ----------

import org.apache.spark.sql.functions._
val result_df = df.groupBy("actor_id","director_id").agg(count(col("timestamp")).as("no_movies"))
.where("no_movies>=3").select("actor_id","director_id")
result_df.show(10,false)
