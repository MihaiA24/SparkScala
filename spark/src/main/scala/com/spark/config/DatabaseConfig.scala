package com.spark.config

import com.typesafe.config.ConfigFactory

import java.io.File

object DatabaseConfig {
  def apply: DatabaseConfig = {
    val appConfig = ConfigFactory.parseFile(new File("spark/src/main/conf/database.conf"))
    DatabaseConfig(
      PostgresSqlConfig(
        url = appConfig.getString("postgres.url"),
        user = appConfig.getString("postgres.user"),
        password = appConfig.getString("postgres.password")
      ),
      PhoenixConfig(zkUrl = appConfig.getString("phoenix.zkUrl")),
      HdfsConfig(
        url = appConfig.getString("hdfs.url"),
        port = appConfig.getString("hdfs.port"))
    )
  }
}

final case class DatabaseConfig(postgresSqlConfig: PostgresSqlConfig, phoenixConfig: PhoenixConfig, hdfsConfig: HdfsConfig)