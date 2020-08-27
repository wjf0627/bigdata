package com.jinfeng.clickhouse

import java.util.{Random, UUID}

import com.jinfeng.clickhouse.entity.Device
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
class CKExample extends Serializable {

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

      val sc = spark.sparkContext
      sc.hadoopConfiguration.set("dfs.client.socket-timeout", "300000")
      // clickhouse params
      val host = "localhost"
      val db = "dwh"
      val tableName = "audience_merge"
      //  cluster configuration must be defined in config.xml (clickhouse config)
      //  val clusterName = Some("localhost"): Option[String]
      val clusterName = None


      val random = new Random
      import spark.implicits._
      // define clickhouse datasource
      implicit val clickhouseDataSource = ClickHouseConnectionFactory.get(host)
      val df = sc.parallelize(1 to 2000000).map(f = _ => {
        Device("" + UUID.randomUUID(),
          Set(random.nextInt(10), random.nextInt(10), random.nextInt(10), random.nextInt(10), random.nextInt(10), random.nextInt(10))
        )
      }).toDF()

      val date_time = "2020081410"
      val date = date_time.substring(0, 8)
      val hour = date_time.substring(8, 10)
      println(date)
      println(hour)
      //  df.createClickHouseDb(db, clusterName)
      //  df.createClickHouseTable(db, tableName, Seq("dt"), Seq("device_id", "offer_id"), Seq(), clusterName)
      df.saveToClickHouse(db, tableName, Seq("2020-08-15", "10"), Seq("dt", "hour"), clusterName)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}


object CKExample {
  def main(args: Array[String]): Unit = {
    new CKExample().run(args)
  }
}