// Databricks notebook source
/*
Input Table:
Employee table:
+----+--------+
| id | salary |
+----+--------+
| 1  | 100    |
| 2  | 200    |
| 3  | 300    |
+----+--------+

Output: 
+---------------------+
| SecondHighestSalary |
+---------------------+
| 200                 |
+---------------------+
Write a solution to find the second highest distinct salary from the Employee table. 
If there is no second highest salary, return null 
*/
// Define the Employee table data
val employeeData = Seq(
  (1, 100),
  (2, 200),
  (3, 300)
)

// Convert the data to an RDD
val employeeRdd = spark.sparkContext.parallelize(employeeData)

// Convert the RDD to a DataFrame with column names
val employeeDF = employeeRdd.toDF("id", "salary")

// Show the Employee DataFrame
employeeDF.show(false)

// COMMAND ----------

import org.apache.spark.sql.functions.col 
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.expressions.Window
val w=Window.orderBy(col("salary").desc)
val rankedDF = employeeDF.withColumn("rank", dense_rank().over(w)).where("rank=2").select(col("salary").as("SecondHighestSalary"))
rankedDF.show(10,false)

// COMMAND ----------


