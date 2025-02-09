// Databricks notebook source
/*
Assume you're given the tables containing completed trade orders and user details in a Robinhood trading system.

Write a query to retrieve the top three cities that have the highest number of completed trade orders listed in descending order. Output the city name and the corresponding number of completed trade orders.

trades Table:
Column Name	Type
order_id	integer
user_id	integer
quantity	integer
status	string ('Completed', 'Cancelled')
date	timestamp
price	decimal (5, 2)
trades Example Input:
+--------+-------+--------+---------+-------------------+-----+
|order_id|user_id|quantity|status   |date               |price|
+--------+-------+--------+---------+-------------------+-----+
|100101  |111    |10      |Cancelled|2022-08-17 12:00:00|9.8  |
|100102  |111    |10      |Completed|2022-08-17 12:00:00|10.0 |
|100259  |148    |35      |Completed|2022-08-25 12:00:00|5.1  |
|100264  |148    |40      |Completed|2022-08-26 12:00:00|4.8  |
|100305  |300    |15      |Completed|2022-09-05 12:00:00|10.0 |
|100400  |178    |32      |Completed|2022-09-17 12:00:00|12.0 |
|100565  |265    |2       |Completed|2022-09-27 12:00:00|8.7  |
+--------+-------+--------+---------+-------------------+-----+
users Table:
Column Name	Type
user_id	integer
city	string
email	string
signup_date	datetime
users Example Input:
+-------+-------------+--------------------+-------------------+
|user_id|         city|               email|        signup_date|
+-------+-------------+--------------------+-------------------+
|    111|San Francisco|    rrok10@gmail.com|2021-08-03 12:00:00|
|    148|       Boston|sailor9820@gmail.com|2021-08-20 12:00:00|
|    178|San Francisco|harrypotterfan182...|2022-01-05 12:00:00|
|    265|       Denver|shadower_@hotmail...|2022-02-26 12:00:00|
|    300|San Francisco|houstoncowboy1122...|2022-06-30 12:00:00|
+-------+-------------+--------------------+-------------------+
Example Output:
+-------------+------------+
|city         |total_orders|
+-------------+------------+
|San Francisco|3           |
|Boston       |2           |
|Denver       |1           |
+-------------+------------+
In the given dataset, San Francisco has the highest number of completed trade orders with 3 orders. Boston holds the second position with 2 orders, and Denver ranks third with 1 order.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val trades_schema = StructType(Seq(
  StructField("order_id", IntegerType, nullable = false),
  StructField("user_id", IntegerType, nullable = false),
  StructField("quantity", IntegerType, nullable = false),
  StructField("status", StringType, nullable = false),
  StructField("date", StringType, nullable = false),
  StructField("price", DoubleType, nullable = false)
))

// Sample data
val trades_data = Seq(
  Row(100101, 111, 10, "Cancelled", "2022-08-17 12:00:00", 9.80),
  Row(100102, 111, 10, "Completed", "2022-08-17 12:00:00", 10.00),
  Row(100259, 148, 35, "Completed", "2022-08-25 12:00:00", 5.10),
  Row(100264, 148, 40, "Completed", "2022-08-26 12:00:00", 4.80),
  Row(100305, 300, 15, "Completed", "2022-09-05 12:00:00", 10.00),
  Row(100400, 178, 32, "Completed", "2022-09-17 12:00:00", 12.00),
  Row(100565, 265, 2, "Completed", "2022-09-27 12:00:00", 8.70)
)

val trades_df = spark.createDataFrame(spark.sparkContext.parallelize(trades_data), trades_schema)
  .withColumn("date",col("date").cast("timestamp"))
trades_df.show(10,false)


// COMMAND ----------



// COMMAND ----------

 

val user_schema = StructType(Seq(
  StructField("user_id", IntegerType, nullable = false),
  StructField("city", StringType, nullable = false),
  StructField("email", StringType, nullable = false),
  StructField("signup_date", StringType, nullable = false)
))

// Sample data
val user_data = Seq(
  Row(111, "San Francisco", "rrok10@gmail.com", "2021-08-03 12:00:00"),
  Row(148, "Boston", "sailor9820@gmail.com", "2021-08-20 12:00:00"),
  Row(178, "San Francisco", "harrypotterfan182@gmail.com", "2022-01-05 12:00:00"),
  Row(265, "Denver", "shadower_@hotmail.com", "2022-02-26 12:00:00"),
  Row(300, "San Francisco", "houstoncowboy1122@hotmail.com", "2022-06-30 12:00:00")
)

val user_df = spark.createDataFrame(spark.sparkContext.parallelize(user_data), user_schema)
  .withColumn("signup_date",col("signup_date").cast("timestamp"))
user_df.show()


// COMMAND ----------

val joined_df = trades_df.join(user_df,Seq("user_id"),"inner").where("status='Completed'")
.select("city","order_id")

// COMMAND ----------


val w = Window.partitionBy("city") 
val w1 = Window.orderBy(col("total_orders").desc)
val agg_df = joined_df.withColumn("total_orders",count(col("order_id")).over(w))
.select("city","total_orders").distinct()
.withColumn("rank",dense_rank().over(w1))
val result_df = agg_df.where("rank<=3").drop("rank")

// COMMAND ----------

result_df.show(10,false)

// COMMAND ----------


