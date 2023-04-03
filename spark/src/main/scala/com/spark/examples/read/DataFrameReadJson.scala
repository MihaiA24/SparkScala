package com.spark.examples.read

import com.spark.utils.ResourceStructType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameReadJson {

  def main(array: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("DataFrameReadJson")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val basePath : String = "spark/src/main/resources/json/"

    // Read simple JSON
    val df : DataFrame = spark.read
      .option("multiline",value = true) // En caso de que el json sea un array [{..},{...}], necesario ponerlo
      // para que identifique la primera y última linea
      .json(basePath + "zipcodes.json")
    df.printSchema()
    df.show()

    // Read multiple files
    val df2 = spark.read.json(
      basePath + "zipcodes_streaming/zipcode1.json",
      basePath + "zipcodes_streaming/zipcode2.json")
    df.printSchema()
    df2.show(false)

    // Read all files from a folder
    val df3 = spark.read.json(basePath + "zipcodes_streaming/*")
    df.printSchema() // Spark auto inferSchema (may be incorrectly)
    df3.show(false)

    // Read Multile JSON with schema
    val zipCodeSchema : StructType = ResourceStructType.ZipCodeMultiLineSchema
    val dfWithSchema : DataFrame = spark.read
      .option("multiline", value = true)
      .schema(schema = zipCodeSchema) // Json Data can also be read with a specific schema
      .json(basePath + "multiline-zipcode.json")

    dfWithSchema.printSchema()
    dfWithSchema.show()

    // Create and read temporary view
    spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS (path 'spark/src/main/resources/json/zipcodes.json')")
    // TODO: No lee correctamente multilineas, se puede hacer en el SQL CONTEXT?
    spark.sqlContext.sql("SELECT * FROM zipcode").show()
    spark.sqlContext.sql("SELECT * FROM zipcode WHERE City = 'MESA' ").show()

    //Write json file

    //df2.write.json("tmp/spark_output/zipcodes.json")

  }
}