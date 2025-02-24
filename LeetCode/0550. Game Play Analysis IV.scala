// Databricks notebook source
/*
0550. Game Play Analysis IV
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
 

Write a solution to report the fraction of players that logged in again on the day after the day they first logged in, rounded to 2 decimal places. In other words, you need to count the number of players that logged in for at least two consecutive days starting from their first login date, then divide that number by the total number of players.

The result format is in the following example.

 

Example 1:

Input: 
Activity table:
+-----------+-----------+------------+--------------+
| player_id | device_id | event_date | games_played |
+-----------+-----------+------------+--------------+
| 1         | 2         | 2016-03-01 | 5            |
| 1         | 2         | 2016-03-02 | 6            |
| 2         | 3         | 2017-06-25 | 1            |
| 3         | 1         | 2016-03-02 | 0            |
| 3         | 4         | 2018-07-03 | 5            |
+-----------+-----------+------------+--------------+
Output: 
+-----------+
| fraction  |
+-----------+
| 0.33      |
+-----------+
Explanation: 
Only the player with id 1 logged back in after the first day he had logged in so the answer is 1/3 = 0.33
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val activity_schema = StructType(Array(
  StructField("player_id", IntegerType, nullable = false),
  StructField("device_id", IntegerType, nullable = false),
  StructField("event_date", StringType, nullable = false),
  StructField("games_played", IntegerType, nullable = false)
))

 
val activity_data = Seq(
  Row(1, 2, "2016-03-01", 5),
  Row(1, 2, "2016-03-02", 6),
  Row(2, 3, "2017-06-25", 1),
  Row(3, 1, "2016-03-02", 0),
  Row(3, 4, "2018-07-03", 5)
)
val activity_rdd = spark.sparkContext.parallelize(activity_data)
val activity_df = spark.createDataFrame(activity_rdd, activity_schema)
.withColumn("event_date",col("event_date").cast("date"))
 
activity_df.show()
activity_df.printSchema()
 


// COMMAND ----------

val w = Window.partitionBy("player_id").orderBy("event_date")
val initial_activity_df = activity_df.withColumn("session",row_number().over(w)).where("session in (1,2)")


// COMMAND ----------

val w1 = Window.partitionBy("player_id").orderBy("event_date")
val login_df = initial_activity_df.withColumn("second_login",lead(col("event_date"),1).over(w1)).where("session=1")

// COMMAND ----------

login_df.show(100,false)

// COMMAND ----------

val return_player = login_df.where("datediff(second_login,event_date)=1").select("player_id").distinct().count()

// COMMAND ----------

val all_player = login_df.select("player_id").distinct().count()

// COMMAND ----------

val fraction =  return_player*1.0/all_player 

// COMMAND ----------


