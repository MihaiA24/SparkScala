package com.spark.dataframe.utils

import org.apache.spark.sql.types._

object ResourceStructType {

  def StreamCsvStructType: StructType = {
    StructType(Array(
      StructField("TotalCost", IntegerType, nullable = true),
      StructField("BirthDate", DateType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("TotalChildren", IntegerType, nullable = true),
      StructField("ProductCategoryName", StringType, nullable = true)
    ))
  }

  def ZipCodesJsonStructType: StructType = {
    val schema = new StructType()
    schema.add("City", StringType, nullable = true)
      .add("Country", StringType, nullable = true)
      .add("Decommisioned", BooleanType, nullable = true)
      .add("EstimatedPopulation", LongType, nullable = true)
      .add("Lat", DoubleType, nullable = true)
      .add("Location", StringType, nullable = true)
      .add("LocationText", StringType, nullable = true)
      .add("LocationType", StringType, nullable = true)
      .add("Long", DoubleType, nullable = true)
      .add("Notes", StringType, nullable = true)
      .add("RecordNumber", LongType, nullable = true)
      .add("State", StringType, nullable = true)
      .add("TaxReturnsFiled", LongType, nullable = true)
      .add("TotalWages", LongType, nullable = true)
      .add("WorldRegion", StringType, nullable = true)
      .add("Xaxis", DoubleType, nullable = true)
      .add("Yaxis", DoubleType, nullable = true)
      .add("Zaxis", DoubleType, nullable = true)
      .add("Zipcode", StringType, nullable = true)
      .add("ZipCodeType", StringType, nullable = true)
  }


}