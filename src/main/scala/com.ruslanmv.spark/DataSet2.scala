
package com.ruslanmv.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//Create Empty Dataset Example
object DataSet2 extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Spark")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR");
  import spark.implicits._

  val schema = StructType(
    StructField("firstName", StringType, true) ::
      StructField("lastName", IntegerType, false) ::
      StructField("middleName", IntegerType, false) :: Nil)

  val colSeq = Seq("firstName","lastName","middleName")

  case class Name(firstName: String, lastName: String, middleName:String)
  case class Empty()
  val ds0 = spark.emptyDataset[Empty]
  ds0.printSchema()

  val ds1=spark.emptyDataset[Name]
  ds1.printSchema()

  val ds2 = spark.createDataset(Seq.empty[Name])
  ds2.printSchema()

  val ds4=spark.createDataset(spark.sparkContext.emptyRDD[Name])
  ds4.printSchema()

  val ds3=spark.createDataset(Seq.empty[(String,String,String)])
  ds3.printSchema()
  val ds5=Seq.empty[(String,String,String)].toDS()
  ds5.printSchema()

  val ds6=Seq.empty[Name].toDS()
  ds6.printSchema()
}