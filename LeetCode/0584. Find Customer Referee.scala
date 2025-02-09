// Databricks notebook source
/*
Table: Customer

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
| referee_id  | int     |
+-------------+---------+
In SQL, id is the primary key column for this table.
Each row of this table indicates the id of a customer, their name, and the id of the customer who referred them.
 

Find the names of the customer that are not referred by the customer with id = 2.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Customer table:
+----+------+------------+
| id | name | referee_id |
+----+------+------------+
| 1  | Will | null       |
| 2  | Jane | null       |
| 3  | Alex | 2          |
| 4  | Bill | null       |
| 5  | Zack | 1          |
| 6  | Mark | 2          |
+----+------+------------+
Output: 
+------+
| name |
+------+
| Will |
| Jane |
| Bill |
| Zack |
+------+
*/

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

 

// Define schema for Customer table
val customer_schema = StructType(Seq(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("referee_id", IntegerType, nullable = true) // Allow NULL for direct customers
))

// Sample data
val customer_data = Seq(
  Row(1, "Will", null),
  Row(2, "Jane", null),
  Row(3, "Alex", 2),
  Row(4, "Bill", null),
  Row(5, "Zack", 1),
  Row(6, "Mark", 2)
)
 
val customer_df = spark.createDataFrame(spark.sparkContext.parallelize(customer_data), customer_schema)
customer_df.show()


// COMMAND ----------

val result_df = customer_df.where("referee_id!=2 or referee_id is null").select("name")
result_df.show(10,false)
