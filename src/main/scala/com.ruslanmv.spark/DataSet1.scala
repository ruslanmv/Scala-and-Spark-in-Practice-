package com.ruslanmv.spark
import org.apache.spark.sql.SparkSession



//We create Simple Dataset
object DataSet1 {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkApp")
      .getOrCreate()



    val Data = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )

    // up to now have created our Dataset

    println(Data)



  }
}