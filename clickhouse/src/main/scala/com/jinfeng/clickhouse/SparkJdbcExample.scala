package com.jinfeng.clickhouse

import java.util.{Properties, Random, UUID}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @package: com.jinfeng.clickhouse
 * @author: wangjf
 * @date: 2019/5/20
 * @time: 下午3:16
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
case class DeviceA(device_id: String, offer_id: Array[Int], region: String) extends Serializable

class SparkJdbcExample extends Serializable {

  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CKExample")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val url = "jdbc:clickhouse://localhost:8123/wangjf"
      val driver = "ru.yandex.clickhouse.ClickHouseDriver"
      val table = "device_test"
      val sc = spark.sparkContext
      val random = new Random
      import spark.implicits._

      val df = sc.parallelize(1 to 10000000).map(f = _ => {
        DeviceA("" + UUID.randomUUID(), Array(random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20),
          random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20)), "")
      }).toDF()

      val properties = new Properties()
      properties.setProperty("driver", driver)

      properties.setProperty("socket_timeout", "300000")
      properties.setProperty("rewriteBatchedStatements", "true")
      properties.setProperty("batchsize", "1000000")
      properties.setProperty("numPartitions", "1")
      properties.setProperty("user", "wangjf")
      properties.setProperty("password", "wangjf")

      df.write.mode(SaveMode.Append).jdbc(url = url, table = table, properties)
      /*
      val host = "localhost"
      val db = "wangjf"
      val tableName = "device_set"
      val clusterName = None

      val random = new Random
      import spark.implicits._
      implicit val clickhouseDataSource = ClickHouseConnectionFactory.get(host)
      val df = sc.parallelize(1 to 10000000).map(f = _ => {
        Device("" + UUID.randomUUID(), Array(random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20),
          random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20), random.nextInt(20)))
      }).toDF()

      df.saveToClickHouse(db, tableName, _ => java.sql.Date.valueOf("2019-07-15"), "dt", clusterName)
       */

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object SparkJdbcExample {
  def main(args: Array[String]): Unit = {
    new SparkJdbcExample().run(args)
  }
}


