package com.ruslanmv.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//Creating Empty DataFrame
object DataFrame6 extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()
  import spark.implicits._


  val schema = StructType(
    StructField("firstName", StringType, true) ::
      StructField("lastName", IntegerType, false) ::
      StructField("middleName", IntegerType, false) :: Nil)

  val colSeq = Seq("firstName","lastName","middleName")

  case class Name(firstName: String, lastName: String, middleName:String)

  // Create empty dataframe using StructType schema
  val df = spark.createDataFrame(spark.sparkContext
    .emptyRDD[Row], schema)

  // Using implicit encoder
  Seq.empty[(String,String,String)].toDF(colSeq:_*)

  //Using case class

  Seq.empty[Name].toDF().printSchema()

  //Using emptyDataFrame
  spark.emptyDataFrame


  //Using emptyDataset


}

