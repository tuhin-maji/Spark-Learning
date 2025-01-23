// Databricks notebook source
//RDD Creation
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CreateRDD").getOrCreate()
val data = Seq(1, 2, 3, 4, 5)
val rdd1 = spark.sparkContext.parallelize(data)
//filter 
//Filter only even numbers
val filtered_rdd = rdd1.filter(x=>x%2==0)
filtered_rdd.collect().foreach(println)

 

