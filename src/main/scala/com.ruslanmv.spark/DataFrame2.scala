package com.ruslanmv.spark
import org.apache.spark.sql._
object DataFrame2 {
  def main(args: Array[String]): Unit = {
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val Data = Seq(("James", 34, "true", "M", "3000.6089"),
      ("Michael", 33, "true", "F", "3300.8067"),
      ("Robert", 37, "false", "M", "5000.5034")
    )
    import spark.implicits._

    val df = Data.toDF("firstname", "age", "isGraduated", "gender", "salary")
    df.printSchema()
    //spark.stop()
  }
}

