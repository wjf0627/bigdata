package com.jinfeng.clickhouse

import java.util.{Random, UUID}

import com.jinfeng.clickhouse.entity.DeviceJson
import com.jinfeng.clickhouse.util.ClickHouseConnectionFactory
import com.jinfeng.clickhouse.util.ClickHouseSparkExt._
import org.apache.spark.sql.SparkSession

/**
  * @package: com.jinfeng.clickhouse
  * @author: wangjf
  * @date: 2019/5/20
  * @time: 下午3:16
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
class JsonExample extends Serializable {

  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("JsonExample")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val sc = spark.sparkContext
      //  sc.hadoopConfiguration.set("dfs.client.socket-timeout", "300000")
      //  clickhouse params
      val host = "localhost"
      val db = "dwh"
      val tableName = "old2new_tag"
      //  cluster configuration must be defined in config.xml (clickhouse config)
      //  val clusterName = Some("localhost"): Option[String]
      val clusterName = None

      val random = new Random
      import spark.implicits._
      // define clickhouse datasource
      implicit val clickhouseDataSource = ClickHouseConnectionFactory.get(host)
      val df = sc.parallelize(1 to 10000000).map(f = _ => {
        DeviceJson("" + UUID.randomUUID(), s"""{"8":${random.nextInt(7)},"4":${random.nextInt(7)},"61":${random.nextInt(7)},"97":${random.nextInt(7)},"297":${random.nextInt(7)},"25":${random.nextInt(7)}}""")
      }).toDF()

      //  df.createClickHouseDb(db, clusterName)
      //  df.createClickHouseTable(db, tableName, "dt", Seq("device_id", "offer_id"), clusterName)
      //  df.saveToClickHouse(db, tableName, _ => java.sql.Date.valueOf("2019-07-15"), "dt", clusterName)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object JsonExample {
  def main(args: Array[String]): Unit = {
    new JsonExample().run(args)
  }
}
