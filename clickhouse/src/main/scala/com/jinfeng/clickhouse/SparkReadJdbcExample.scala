package com.jinfeng.clickhouse

import java.util.{Properties, Random}

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @package: com.jinfeng.clickhouse
  * @author: wangjf
  * @date: 2019/5/20
  * @time: 下午3:16
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
class SparkReadJdbcExample extends Serializable {

  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("SparkReadJdbcExample")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val url = "jdbc:clickhouse://localhost:8123/"
      val driver = "ru.yandex.clickhouse.ClickHouseDriver"
      val database = "wangjf"
      val table = "device_json"
      val sc = spark.sparkContext
      val random = new Random

      val properties = new Properties()
      properties.setProperty("driver", driver)

      properties.setProperty("socket_timeout", "300000")
      properties.setProperty("rewriteBatchedStatements", "true")
      properties.setProperty("batchsize", "1000000")
      properties.setProperty("numPartitions", "1")
      //  properties.setProperty("user", "wangjf")
      //  properties.setProperty("password", "wangjf")

      val df = jdbcConnection(spark, url, database, table, properties).take(10)
      df.foreach(println)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  def jdbcConnection(spark: SparkSession, url: String, database: String, table: String, properties: Properties): DataFrame = {
    spark.read.jdbc(url = url + database, table = table, properties = properties)
  }
}

object SparkReadJdbcExample {
  def main(args: Array[String]): Unit = {
    new SparkReadJdbcExample().run(args)
  }
}