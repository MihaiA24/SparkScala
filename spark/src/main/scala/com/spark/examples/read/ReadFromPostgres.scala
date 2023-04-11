package com.spark.examples.read

import com.spark.SparkApp
import com.spark.config.DatabaseConfig
import com.spark.utils.ReadDataFromDB
import com.spark.utils.SQLConstants._
import org.apache.spark.sql.SparkSession


object ReadFromPostgres {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession  = SparkApp.createSparkSession("ReadFromPostgres")

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