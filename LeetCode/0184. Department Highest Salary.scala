// Databricks notebook source
/*
Table: Employee

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| id           | int     |
| name         | varchar |
| salary       | int     |
| departmentId | int     |
+--------------+---------+
id is the primary key (column with unique values) for this table.
departmentId is a foreign key (reference columns) of the ID from the Department table.
Each row of this table indicates the ID, name, and salary of an employee. It also contains the ID of their department.
 

Table: Department

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
+-------------+---------+
id is the primary key (column with unique values) for this table. It is guaranteed that department name is not NULL.
Each row of this table indicates the ID of a department and its name.
 

Write a solution to find employees who have the highest salary in each of the departments.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employee table:
+----+-------+--------+--------------+
| id | name  | salary | departmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Jim   | 90000  | 1            |
| 3  | Henry | 80000  | 2            |
| 4  | Sam   | 60000  | 2            |
| 5  | Max   | 90000  | 1            |
+----+-------+--------+--------------+
Department table:
+----+-------+
| id | name  |
+----+-------+
| 1  | IT    |
| 2  | Sales |
+----+-------+
Output: 
+------------+----------+--------+
| Department | Employee | Salary |
+------------+----------+--------+
| IT         | Jim      | 90000  |
| Sales      | Henry    | 80000  |
| IT         | Max      | 90000  |
+------------+----------+--------+
Explanation: Max and Jim both have the highest salary in the IT department and Henry has the highest salary in the Sales department.
*/

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank,col,desc}
val emp_schema = StructType(Seq(
  StructField("id", IntegerType, false),
  StructField("name", StringType, false),
  StructField("salary", IntegerType, false),
  StructField("departmentId", IntegerType, false)
))

// Create data
val emp_data = Seq(
  Row(1, "Joe", 70000, 1),
  Row(2, "Jim", 90000, 1),
  Row(3, "Henry", 80000, 2),
  Row(4, "Sam", 60000, 2),
  Row(5, "Max", 90000, 1)
)
val emp_rdd = spark.sparkContext.parallelize(emp_data)
// Create DataFrame
val emp_df: DataFrame = spark.createDataFrame(emp_rdd, emp_schema)

emp_df.show()


// COMMAND ----------

val department_schema = StructType(Seq(
  StructField("id", IntegerType, false),
  StructField("name", StringType, false)
))

 
// Create department data
val department_data = Seq(
  Row(1, "IT"),
  Row(2, "Sales")
)

val department_rdd = spark.sparkContext.parallelize(department_data)
val department_df: DataFrame = spark.createDataFrame(department_rdd, department_schema)
val dept_df = department_df.select(col("id").as("departmentId"),col("name").as("departmentName"))
 
dept_df.show()

// COMMAND ----------

val emp_dept_df = emp_df.join(dept_df,Seq("departmentId"),"inner")
emp_dept_df.show(10,false)

// COMMAND ----------

val w = Window.partitionBy("departmentId").orderBy(col("salary").desc)

val emp_ranked_df = emp_dept_df.withColumn("rank",dense_rank().over(w))
val top_emp_df = emp_ranked_df.where("rank=1").select("departmentName","name","salary")
top_emp_df.show(10,false)

// COMMAND ----------

nam
