// Databricks notebook source
/*
Table: Weather

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| recordDate    | date    |
| temperature   | int     |
+---------------+---------+
id is the column with unique values for this table.
There are no different rows with the same recordDate.
This table contains information about the temperature on a certain day.
 

Write a solution to find all dates' id with higher temperatures compared to its previous dates (yesterday).

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Weather table:
+----+------------+-------------+
| id | recordDate | temperature |
+----+------------+-------------+
| 1  | 2015-01-01 | 10          |
| 2  | 2015-01-02 | 25          |
| 3  | 2015-01-03 | 20          |
| 4  | 2015-01-04 | 30          |
+----+------------+-------------+
Output: 
+----+
| id |
+----+
| 2  |
| 4  |
+----+
Explanation: 
In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
In 2015-01-04, the temperature was higher than the previous day (20 -> 30).
 
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
 
val schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("recordDate", DateType, nullable = false),
  StructField("temperature", IntegerType, nullable = false)
))

// Create data
val data = Seq(
  (1, "2015-01-01", 10),
  (2, "2015-01-02", 25),
  (3, "2015-01-03", 20),
  (4, "2015-01-04", 30)
)

// Convert sequence to DataFrame
val df = data.toDF("id", "recordDate", "temperature")
  .withColumn("recordDate", $"recordDate".cast("date")) // Ensure DateType

// Show DataFrame
df.show()


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val w=Window.orderBy("recordDate")
val computed_df = df.withColumn("yesterday_temperature",lag(col("temperature"),1).over(w))
.withColumn("yesterday",lag(col("recordDate"),1).over(w))
.withColumn("date_diff",datediff(col("recordDate"),col("yesterday")))
computed_df.show(10,false)

// COMMAND ----------

val result_df = computed_df.where("temperature> yesterday_temperature and date_diff=1").select("id")
 
result_df.show(10,false)
