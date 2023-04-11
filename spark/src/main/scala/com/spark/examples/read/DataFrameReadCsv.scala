package com.spark.examples.read

import com.spark.SparkApp
import com.spark.utils.ResourceStructType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameReadCsv {
  def main(array: Array[String]): Unit = {

    val spark: SparkSession = SparkApp.createSparkSession("DataFrameReadCsv")

    val filePath = "spark/src/main/resources/csv/stream.csv"

    val streamSchema: StructType = ResourceStructType.StreamCsvStructType
    // Read CSV Method 1
    val df: DataFrame = readCsvMethod1(spark, filePath, streamSchema)

    df.printSchema()
    df.show()

    // Read CSV Method2. Options can be also a map
    val df2: DataFrame = readCsvMethod2(spark, filePath, streamSchema)

    df2.printSchema()
    df2.show()


    // Read CSV Method 3
    val df3: DataFrame = readCsvMethod3(spark, filePath, streamSchema)

    df3.printSchema()
    df3.show()

  }

  private def readCsvMethod1(spark: SparkSession, filePath: String, streamSchema: StructType): DataFrame = {
    spark.read
      .option("header", value = true)
      .option("delimiter", "|")
      // Evalua cada columna para no definirlos todos como string. En caso de usar schema no es necesario
      //      .option("inferSchema",value = true)
      .schema(schema = streamSchema)
      .csv(filePath)
  }


  private def readCsvMethod2(spark: SparkSession, filePath: String, streamSchema: StructType): DataFrame = {
    spark.read
      .options(Map("header" -> "true", "delimiter" -> "|"))
      .schema(schema = streamSchema)
      .csv(filePath)
  }

  private def readCsvMethod3(spark: SparkSession, filePath: String, streamSchema: StructType): DataFrame = {
    spark.read
      .option("header", value = true)
      .option("delimiter", "|")
      .schema(schema = streamSchema)
      .format("csv")
      .load(filePath)
  }
}