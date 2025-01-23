// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
  .appName("RDD groupByKey Example")
  .master("local[*]")  
  .getOrCreate()
 
val sc = spark.sparkContext

// Create an RDD of key-value pairs
val data = List(1, 2, 3, 4, 5)
val rdd =sc.parallelize(data)
val firstElement = rdd.first()
println(firstElement)

// COMMAND ----------


