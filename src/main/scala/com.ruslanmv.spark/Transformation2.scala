package com.ruslanmv.spark
import org.apache.spark.sql.SparkSession


// Reduce By Key Example
object Transformation2 extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()

  val data = Seq(("Project", 1),
    ("Gutenberg’s", 1),
    ("Alice’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1))

  val rdd = spark.sparkContext.parallelize(data)

  val rdd2 = rdd.reduceByKey(_ + _)

  rdd2.foreach(println)
}