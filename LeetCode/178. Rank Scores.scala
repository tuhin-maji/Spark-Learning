// Databricks notebook source
/*
Input: 
Scores table:
+----+-------+
| id | score |
+----+-------+
| 1  | 3.50  |
| 2  | 3.65  |
| 3  | 4.00  |
| 4  | 3.85  |
| 5  | 4.00  |
| 6  | 3.65  |
+----+-------+
Output: 
+-------+------+
| score | rank |
+-------+------+
| 4.00  | 1    |
| 4.00  | 1    |
| 3.85  | 2    |
| 3.65  | 3    |
| 3.65  | 3    |
| 3.50  | 4    |
+-------+------+

Write a solution to find the rank of the scores. The ranking should be calculated according to the following rules:

The scores should be ranked from the highest to the lowest.
If there is a tie between two scores, both should have the same ranking.
After a tie, the next ranking number should be the next consecutive integer value. In other words, there should be no holes between ranks.
Return the result table ordered by score in descending order.
*/

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
 
// Define the data for the table
val scoresData = Seq(
  (1, 3.50),
  (2, 3.65),
  (3, 4.00),
  (4, 3.85),
  (5, 4.00),
  (6, 3.65)
)

// Convert the data to a DataFrame
val scoresDF = scoresData.toDF("id", "score")

// Show the initial DataFrame
scoresDF.show()


// COMMAND ----------

val w=Window.orderBy(col("score").desc)
val rankedData =scoresDF.withColumn("rank", dense_rank.over(w)).select("score","rank")
rankedData.orderBy("rank").show(10,false)


// COMMAND ----------


