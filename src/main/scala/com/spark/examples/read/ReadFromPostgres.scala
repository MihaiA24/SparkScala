package com.spark.examples.read

import com.spark.config.DatabaseConfig
import com.spark.utils.SQLConstants._
import com.spark.utils.ReadDataFromDB
import org.apache.spark.sql.SparkSession


object ReadFromPostgres {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("DataFrameUsingCsv")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") // Clean data output
    val ReadDataFromDB = new ReadDataFromDB(spark, DatabaseConfig.apply)

    val queryArgs = List(
      ("name", "One", OR),
      ("duration", "2 day", null)
    )
    val cols = Seq("name", "duration")
    val df = ReadDataFromDB.getDataFromJDBC(table = "courses",cols = cols, conditions = queryArgs)
    df.printSchema()
    df.show(false)
  }
}