// Databricks notebook source
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder
  .appName("RDD groupByKey Example")
  .master("local[*]")  
  .getOrCreate()
 
val sc = spark.sparkContext

// Create an RDD of key-value pairs
val data = List(("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5))
val rdd =sc.parallelize(data)
val reducedRdd = rdd.reduceByKey((x,y)=>x+y)
reducedRdd.collect().foreach{ case (k,sum) => 
  println(s"$k->$sum")
}

// COMMAND ----------


