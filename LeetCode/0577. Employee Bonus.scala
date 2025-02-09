// Databricks notebook source
/*
Table: Employee

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| empId       | int     |
| name        | varchar |
| supervisor  | int     |
| salary      | int     |
+-------------+---------+
empId is the column with unique values for this table.
Each row of this table indicates the name and the ID of an employee in addition to their salary and the id of their manager.
 

Table: Bonus

+-------------+------+
| Column Name | Type |
+-------------+------+
| empId       | int  |
| bonus       | int  |
+-------------+------+
empId is the column of unique values for this table.
empId is a foreign key (reference column) to empId from the Employee table.
Each row of this table contains the id of an employee and their respective bonus.
 

Write a solution to report the name and bonus amount of each employee with a bonus less than 1000.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+-------+--------+------------+--------+
| empId | name   | supervisor | salary |
+-------+--------+------------+--------+
| 3     | Brad   | null       | 4000   |
| 1     | John   | 3          | 1000   |
| 2     | Dan    | 3          | 2000   |
| 4     | Thomas | 3          | 4000   |
+-------+--------+------------+--------+
Bonus table:
+-------+-------+
| empId | bonus |
+-------+-------+
| 2     | 500   |
| 4     | 2000  |
+-------+-------+
Output: 
+------+-------+
| name | bonus |
+------+-------+
| Brad | null  |
| John | null  |
| Dan  | 500   |
+------+-------+
*/

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val emp_schema = StructType(Seq(
  StructField("empId", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("supervisor", IntegerType, nullable = true), // Allow nulls
  StructField("salary", IntegerType, nullable = false)
))

val emp_data = Seq(
  Row(3, "Brad", null, 4000),
  Row(1, "John", 3, 1000),
  Row(2, "Dan", 3, 2000),
  Row(4, "Thomas", 3, 4000)
)

val emp_df = spark.createDataFrame(spark.sparkContext.parallelize(emp_data), emp_schema)
emp_df.show()


// COMMAND ----------

val bonus_schema = StructType(Seq(
  StructField("empId", IntegerType, nullable = false),
  StructField("bonus", IntegerType, nullable = false)
))

// Bonus data
val bonus_data = Seq(
  Row(2, 500),
  Row(4, 2000)
)

// Create Bonus DataFrame
val bonus_df = spark.createDataFrame(spark.sparkContext.parallelize(bonus_data), bonus_schema)
bonus_df.show(10,false)

// COMMAND ----------

val result_df = emp_df.join(bonus_df,Seq("empId"),"left").where("bonus<1000 or bonus is null")
.select("name","bonus")
result_df.show(10,false)
