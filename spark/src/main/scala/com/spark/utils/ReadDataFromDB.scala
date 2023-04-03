package com.spark.utils

import com.spark.config.DatabaseConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

final class ReadDataFromDB(sparkSession: SparkSession, databaseConfig: DatabaseConfig) {
  def getDataFromJDBC(table: String, cols: Seq[String] = Seq("*"), conditions: List[(String, String, String)] = null): DataFrame = {
    val properties = new Properties()
    properties.put("user", databaseConfig.postgresSqlConfig.user)
    properties.put("password", databaseConfig.postgresSqlConfig.password)

    sparkSession.read
      .jdbc(databaseConfig.postgresSqlConfig.url, table, properties)
      .select(cols.map(col): _*)
      .where(parseQuery(conditions))
  }

  def getDataFromHBase(table: String, cols: Seq[String] = Seq("*"), conditions: List[(String, String, String)] = null): DataFrame = {
    sparkSession.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> table, "zkUrl" -> databaseConfig.phoenixConfig.zkUrl))
      .load()
      .select(cols.map(col): _*)
      .where(parseQuery(conditions))
  }

  def readCsvFromHDFS(path: String, header: Boolean = true, delimiter: String = ",") : DataFrame = {
    sparkSession.read
      .option("header", header)
      .option("delimiter", delimiter)
      .csv("hdfs://%s:%s/%s".format(databaseConfig.hdfsConfig.url, databaseConfig.hdfsConfig.port,path))
  }

  private def parseQuery(conditions: List[(String, String, String)]) : String = {
    if (conditions == null)
      return "1=1" // Temporal fix if no where conditions
    val genericCondition = "%s = '%s' %s "
    var finalQuery = ""
    conditions.foreach(condition => finalQuery += genericCondition.format(condition._1, condition._2,
      if (condition._3 != null) condition._3 else ""))
    finalQuery
  }
}