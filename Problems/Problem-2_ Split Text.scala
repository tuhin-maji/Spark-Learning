// Databricks notebook source
//Split one column ad create 3 columns out of it. Data is separated by '-' and always 2 '-' will be there.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
val spark = SparkSession.builder
  .appName("Word Count")
  .master("local[*]") // Use local mode for testing
  .getOrCreate()
 
val sc = spark.sparkContext
val data = List("abc-cda-pqr")
val rdd = sc.parallelize(data)
val df=rdd.toDF("raw_data")
 
val df1=df.withColumn("splited_data",split(col("raw_data"),"-"))
  .withColumn("part_1",col("splited_data")(0))
  .withColumn("part_2",col("splited_data")(1))
  .withColumn("part_3",col("splited_data")(2))
  .drop("splited_data")
df1.show(10,false)
/*
Output:
+-----------+------+------+------+
|raw_data   |part_1|part_2|part_3|
+-----------+------+------+------+
|abc-cda-pqr|abc   |cda   |pqr   |
+-----------+------+------+------+
*/
