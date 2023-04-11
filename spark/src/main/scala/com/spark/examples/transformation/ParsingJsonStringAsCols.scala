package com.spark.examples.transformation

import com.spark.SparkApp
import com.spark.dto.ZipCode
import com.spark.utils.JsonUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object ParsingJsonStringAsCols {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkApp.createSparkSession("ParsingJsonStringAsCols")

    // In this cases, lets supose our data came other source like a Database and the JSON came as plain text
    val df = spark.read
      .text("spark/src/main/resources/json/zipcodes.json")

    df.printSchema()
    df.show()

    // Schema based on object
    val encoder: Encoder[ZipCode] = Encoders.product[ZipCode]
    val schema: StructType = encoder.schema
    // Using from_json -> jsonData will be a struct with the fields of the json
    val dfJson = df.withColumn("jsonData", from_json(col("value"),schema))
    dfJson.printSchema()
    dfJson.show(5, truncate = false)
    // We can select the struct as individual cols:
    val dfJsonCols = dfJson.select("value","jsonData.*")

    dfJsonCols.printSchema()
    dfJsonCols.show(5, truncate = false)
    // Alternative if Spark Version is 1.6 where from_json is not implemented
    val df16 = dfJson.withColumn("jsonData", functions.concat(get_json_object(col("value"), "$.Zipcode"),
      lit(":"), get_json_object(col("value"), "$.RecordNumber")))

    df16.printSchema()
    df16.show()
    // If we want to read from an array of json
    val dfArray = spark.read
      .text("spark/src/main/resources/json/zipcodes_array.json")

    dfArray.printSchema()
    dfArray.show(false)

    // In the case of from_json we can use the reconvert the previous schema to Array[ZipCode]
    val arraySchema = ArrayType(schema)

    val dfJsonArray = dfArray.withColumn("jsonData", from_json(col("value"), arraySchema))
    dfJsonArray.printSchema()
    dfJsonArray.show(5, truncate = false)

    // In case of Spark Version 1.6, we have to add [*] in front of the attribute we want  $.Zipcode -> $[*].Zipcode
    val dfJsonArray16 = dfArray.withColumn("jsonData", functions.concat(get_json_object(col("value"), "$[*].Zipcode"),
      lit(":"), get_json_object(col("value"), "$[*].RecordNumber")))
    dfJsonArray16.printSchema()
    dfJsonArray16.show(false)


    // An easy alternative way is to implement our own JsonUtils
    // We can define and UDF function to parse the way we want
    val parseStringToZipCode = udf((stringJson: String) => {
      val zipCode: ZipCode = JsonUtils.fromJson[ZipCode](stringJson)
      (zipCode.Zipcode , zipCode.RecordNumber)
    })

    val dfJsonUtils = df.withColumn("jsonData", parseStringToZipCode(col("value")))
    dfJsonUtils.printSchema()
    dfJsonUtils.show(false)

    // In Case of Array of Json
    val parseStringToArrayZipcode = udf((stringJson: String) => {
      JsonUtils.fromJson[Array[ZipCode]](stringJson)
        .map(entry => entry.Zipcode -> entry.RecordNumber).toMap
    })

    val dfJsonUtilsArray = dfArray.withColumn("jsonData", parseStringToArrayZipcode(col("value")))
    dfJsonUtilsArray.printSchema()
    dfJsonUtilsArray.show(false)
  }
}