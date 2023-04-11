package com.spark

import org.apache.spark.sql.SparkSession

object SparkApp {
  def createSparkSession(appName: String, logLevel: String = "ERROR"): SparkSession = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(appName)
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }
}