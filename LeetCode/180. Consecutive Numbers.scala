// Databricks notebook source
/*
Input: 
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
Output: 
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+
Find all numbers that appear at least three times consecutively.

Return the result table in any order.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
 
// Define the data
val numData = Seq(
  (1, 1),
  (2, 1),
  (3, 1),
  (4, 2),
  (5, 1),
  (6, 2),
  (7, 2)
)

// Convert the data to a DataFrame
val numDF = numData.toDF("id", "num")

// Show the initial DataFrame
numDF.show()


// COMMAND ----------

val w = Window.orderBy(col("id"))
val numDF1 = numDF.withColumn("num1",lead("num",1).over(w))
.withColumn("num2", lead("num", 2).over(w))
.where("num=num1 and num=num2").select(col("num").as("ConsecutiveNums")).distinct()
numDF1.show(10,false)

// COMMAND ----------


