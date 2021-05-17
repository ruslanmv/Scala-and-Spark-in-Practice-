package com.ruslanmv.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
//ParquetToCsv
object Save2 extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //read parquet file
  val df = spark.read.format("parquet")
    .load("src/main/resources/zipcodes.parquet")
  df.show()
  df.printSchema()

  //convert to csv
  df.write.mode(SaveMode.Overwrite)
    .csv("/tmp/csv/zipcodes.csv")

}
