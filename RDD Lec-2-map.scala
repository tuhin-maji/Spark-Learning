// Databricks notebook source
//RDD Creation
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CreateRDD").getOrCreate()
val data = Seq(1, 2, 3, 4, 5)
val rdd1 = spark.sparkContext.parallelize(data)
//map 
val squared_rdd = rdd1.map(x=>x*x)
squared_rdd.collect().foreach(println)
