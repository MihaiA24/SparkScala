package com.spark.examples.performance

import com.spark.SparkApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructType, StructField}

import scala.util.Random

object ArrayFilterVsBroadcast {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkApp.createSparkSession("ArrayFilterVsBroadcast")

    val schema: StructType = StructType(Array(StructField("Number", IntegerType, nullable = true)))

    val df_list: Seq[Int] = Seq.fill(5000000)(Random.nextInt() * 1000000)

    val df_row_list: RDD[Row] = spark.sparkContext.parallelize(df_list).map(x => Row(x))

    val df_spark: DataFrame = spark.createDataFrame(df_row_list, schema)

    val filter_list: RDD[Row] = spark.sparkContext.parallelize(Array.range(0, 10000)).map(x => Row(x))
    val df_filter: DataFrame = spark.createDataFrame(filter_list, schema)

    df_spark.show(5)
    df_spark.printSchema()
    println("Len df: " + df_spark.count())

    df_filter.show(5)
    df_filter.printSchema()
    println("Len filter df: " + df_filter.count())

    val t1 = System.currentTimeMillis()
    //    val df_result = df_spark.filter(df_spark("Number").isin(df_list:_*))
    df_spark.filter(df_spark("Number").isin(df_list))
    val mod_rdd = df_spark.rdd.filter(x => df_list.contains(x.getInt(0)))
    val df_result = spark.createDataFrame(mod_rdd, schema)
    //    df_spark.rdd.foreach(x => println(x(0)))
    df_result.show(5)
    println("Time: " + ((System.currentTimeMillis() - t1) / 1000))
//    println("Len: " + df_result.count())

    val t2 = System.currentTimeMillis()
    val df_result2 = df_spark.join(broadcast(df_filter), df_spark("Number") <=> df_filter("Number"))
    df_result2.show(5)
    println("Time: " + ((System.currentTimeMillis() - t2) / 1000))
//    println("Len: " + df_result2.count())
  }

}