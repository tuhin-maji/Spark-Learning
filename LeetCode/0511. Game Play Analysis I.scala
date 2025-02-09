// Databricks notebook source
/*
Table: Activity

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| player_id    | int     |
| device_id    | int     |
| event_date   | date    |
| games_played | int     |
+--------------+---------+
(player_id, event_date) is the primary key (combination of columns with unique values) of this table.
This table shows the activity of players of some games.
Each row is a record of a player who logged in and played a number of games (possibly 0) before logging out on someday using some device.
 

Write a solution to find the first login date for each player.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Activity table:
+-----------+-----------+------------+--------------+
| player_id | device_id | event_date | games_played |
+-----------+-----------+------------+--------------+
| 1         | 2         | 2016-03-01 | 5            |
| 1         | 2         | 2016-05-02 | 6            |
| 2         | 3         | 2017-06-25 | 1            |
| 3         | 1         | 2016-03-02 | 0            |
| 3         | 4         | 2018-07-03 | 5            |
+-----------+-----------+------------+--------------+
Output: 
+-----------+-------------+
| player_id | first_login |
+-----------+-------------+
| 1         | 2016-03-01  |
| 2         | 2017-06-25  |
| 3         | 2016-03-02  |
+-----------+-------------+
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
 
val schema = StructType(Seq(
  StructField("player_id", IntegerType, nullable = false),
  StructField("device_id", IntegerType, nullable = false),
  StructField("event_date", DateType, nullable = false),
  StructField("games_played", IntegerType, nullable = false)
))

// Sample data
val data = Seq(
  (1, 2, "2016-03-01", 5),
  (1, 2, "2016-05-02", 6),
  (2, 3, "2017-06-25", 1),
  (3, 1, "2016-03-02", 0),
  (3, 4, "2018-07-03", 5)
)

// Create DataFrame
val df = data.toDF("player_id", "device_id", "event_date", "games_played")
  .withColumn("event_date", $"event_date".cast("date")) // Ensure DateType

// Show DataFrame
df.show()


// COMMAND ----------

import org.apache.spark.sql.functions._

val first_logint_df = df.groupBy("player_id").agg(min(col("event_date")).as("first_login")).orderBy("player_id")
first_logint_df.show(10,false)

// COMMAND ----------

//Approach-II
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val w=Window.partitionBy("player_id").orderBy("event_date")
val first_logint_df = df.withColumn("session_id",row_number().over(w)).where("session_id=1")
.select(col("player_id"),col("event_date").as("first_login"))
first_logint_df.show(10,false)
