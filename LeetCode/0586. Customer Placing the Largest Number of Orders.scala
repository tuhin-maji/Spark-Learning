// Databricks notebook source
/*
Table: Orders

+-----------------+----------+
| Column Name     | Type     |
+-----------------+----------+
| order_number    | int      |
| customer_number | int      |
+-----------------+----------+
order_number is the primary key (column with unique values) for this table.
This table contains information about the order ID and the customer ID.
 

Write a solution to find the customer_number for the customer who has placed the largest number of orders.

The test cases are generated so that exactly one customer will have placed more orders than any other customer.

The result format is in the following example.

 

Example 1:

Input: 
Orders table:
+--------------+-----------------+
| order_number | customer_number |
+--------------+-----------------+
| 1            | 1               |
| 2            | 2               |
| 3            | 3               |
| 4            | 3               |
+--------------+-----------------+
Output: 
+-----------------+
| customer_number |
+-----------------+
| 3               |
+-----------------+
Explanation: 
The customer with number 3 has two orders, which is greater than either customer 1 or 2 because each of them only has one order. 
So the result is customer_number 3.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.expressions.Window
val orders_schema = StructType(Array(
  StructField("order_number", IntegerType, nullable = false),
  StructField("customer_number", IntegerType, nullable = false)
))
 
val orders_data = Seq(
  Row(1, 1),
  Row(2, 2),
  Row(3, 3),
  Row(4, 3)
)
val orders_rdd = spark.sparkContext.parallelize(orders_data)
val orders_df = spark.createDataFrame(orders_rdd, orders_schema)
 
orders_df.show()



// COMMAND ----------

val customer_order_agg_df = orders_df.groupBy("customer_number").agg(count(col("order_number")).as("order_count"))

// COMMAND ----------

customer_order_agg_df.show(10,false)

// COMMAND ----------

val w = Window.orderBy(col("order_count").desc)

// COMMAND ----------

val top_customer_df =customer_order_agg_df.withColumn("rank", dense_rank().over(w)).where("rank=1").select("customer_number")
top_customer_df.show(10,false)

// COMMAND ----------


