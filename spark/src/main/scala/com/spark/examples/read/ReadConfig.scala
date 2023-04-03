package com.spark.examples.read

import com.spark.config.DatabaseConfig

object ReadConfig {
  def main(args: Array[String]): Unit = {
    val conf : DatabaseConfig = DatabaseConfig.apply
    println(conf.postgresSqlConfig)
    println(conf.phoenixConfig)
    println(conf.hdfsConfig)

    println("hdfs://%s:%s/%s".format(conf.hdfsConfig.url, conf.hdfsConfig.port,"/import"))
  }
}