package com.spark.examples.read

import com.spark.SparkApp
import com.spark.dto.RandomTestDTO
import com.spark.utils.SchemaModification
import dto.RandomTestJavaDTO
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

object ReadFileAsSparkDataset {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkApp.createSparkSession("ReadFileAsSparkDataset")

    val path: String = "spark/src/main/resources/csv/random/random_1M.csv"

    // Schema can be created in multiple ways

    // Schema created from a Scala Case Class:
    val encoderTestObject: Encoder[RandomTestDTO] = Encoders.product[RandomTestDTO]
    val schemaFromObject: StructType = encoderTestObject.schema

    println("Schema created from object: ")
    schemaFromObject.printTreeString()

    // Schema can be also created from Java Object
    val javaEnconderTestObject: Encoder[RandomTestJavaDTO] = Encoders.bean(classOf[RandomTestJavaDTO])
    val schemaFromJavaTestObject: StructType = javaEnconderTestObject.schema

    println("Schema created from java object: ")
    schemaFromJavaTestObject.printTreeString()

    /* NOTE: Primitives like java int set nullable = true in schema. To change this, we can change type to
    DataTypes that accept nulls like java.lang.Integer or apply a function to convert to nullables:
     */
    println("Schema created from object with all nullables: ")
    SchemaModification.setAllNullableFlags(schemaFromObject, nullableFlag = true).printTreeString()


    // Schema created manually
    val schemaManual: StructType = StructType(Array(
      StructField("uuid", StringType, nullable = false),
      StructField("position", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("time", TimestampType, nullable = false),
      StructField("prob", FloatType, nullable = false),
      StructField("uses", IntegerType, nullable = false)
    ))

    println("Schema created manual: ")
    schemaManual.printTreeString()

    // NOTE: If header is present, is not necessary to specify schema, just: header -> True and as(...).
    val ds : Dataset[RandomTestDTO] = spark.read
      .option("header","false")
      .option("delimiter",";")
      .option("timeStampFormat","yyyy-MM-dd HH:mm:ss")
      .option("inferSchema", "true")
      .schema(schema = schemaFromObject)
      .csv(path = path)
      .as(Encoders.product[RandomTestDTO])

    ds.filter(_.position == "up").show()

    ds.printSchema()
    ds.show()

    /* Se puede operar en el Dataset como si fuera un DataFrame, manteniendo los Tipos. Esto es útil para evitar errores
      en tiempo de compilación y evitarlos en tiempo de ejecución
     */
  }

  def setAllNullableFlags(structType: StructType, nullableFlag: Boolean): StructType = {
    val modifiedFields = structType.fields.map(field => StructField(field.name, field.dataType, nullableFlag))
    StructType(modifiedFields)
  }
}