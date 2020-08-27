package com.jinfeng.clickhouse

import java.util.{Random, UUID}

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
class TagExample extends Serializable {

  protected def run(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("TagExample")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val sc = spark.sparkContext
      sc.hadoopConfiguration.set("dfs.client.socket-timeout", "300000")
      // clickhouse params
      val host = "ip-172-31-30-45.ec2.internal"
      val db = "dwh"
      val tableName = "ods_user_info"
      //  cluster configuration must be defined in config.xml (clickhouse config)
      val clusterName = Some("cluster_1st"): Option[String]
      //  val clusterName = None

      import spark.implicits._
      // define clickhouse datasource
      implicit val clickhouseDataSource = ClickHouseConnectionFactory.get(host)

      val random = new Random
      val df = sc.parallelize(1 to 10000000).map(f = _ => {
        DeviceA("" + UUID.randomUUID(), Array(
          random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
          random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
          random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
          random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
          random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100)),"")
      }).toDF()

      df.createClickHouseDb(db, clusterName)
      df.createClickHouseTable(db, tableName, Seq("dt"), Seq("device_id", "offer_id"), Seq(), clusterName)
      df.saveToClickHouse(db, tableName, Seq("2019-07-15"), Seq("dt"), clusterName)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}


