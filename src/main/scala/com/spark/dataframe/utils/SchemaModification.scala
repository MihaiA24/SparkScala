package com.spark.dataframe.utils

import org.apache.spark.sql.types.{StructType,StructField}

object SchemaModification {
  def setAllNullableFlags(structType: StructType, nullableFlag: Boolean): StructType = {
    val modifiedFields = structType.fields.map(field => StructField(field.name, field.dataType, nullableFlag))
    StructType(modifiedFields)
  }
}