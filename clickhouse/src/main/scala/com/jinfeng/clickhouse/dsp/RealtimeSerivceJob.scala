package com.jinfeng.clickhouse.dsp

/**
  * @package: com.jinfeng.clickhouse.dsp
  * @author: wangjf
  * @date: 2019-10-18
  * @time: 16:21
  * @email: jinfeng.wang@mobvista.com
  * @phone: 152-1062-7698
  */

import java.text.SimpleDateFormat
import java.util.Date

import com.jinfeng.clickhouse.util.ClickHouseConnectionFactory
import com.jinfeng.clickhouse.util.ClickHouseSparkExt._
import org.apache.spark.sql.SparkSession
import ru.yandex.clickhouse.ClickHouseDataSource

import scala.collection.mutable

/**
  * @package: mobvista.dmp.clickhouse.dsp
  * @author: wangjf
  * @date: 2019-10-17
  * @time: 10:20
  * @email: jinfeng.wang@mobvista.com
  * @phone: 152-1062-7698
  */
class RealtimeSerivceJob extends Serializable {

  /*
  def commandOptions(): Options = {
    val options = new Options()
    options.addOption("date", true, "date")
    options.addOption("host", true, "host")
    options.addOption("cluster", true, "cluster")
    options.addOption("database", true, "database")
    options.addOption("table", true, "table")
    options.addOption("input", true, "input")
    options.addOption("region", true, "region")
    options.addOption("hour", true, "hour")
    options
  }
  */

  val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
  val sdf2 = new SimpleDateFormat("yyyyMMdd")

  protected def run(args: Array[String]) {
    /*
    val parser = new BasicParser()
    val options = commandOptions()
    val commandLine = parser.parse(options, args)
    val date = commandLine.getOptionValue("date")
    val cluster = commandLine.getOptionValue("cluster")
    val host = commandLine.getOptionValue("host")
    val database = commandLine.getOptionValue("database")
    val table = commandLine.getOptionValue("table")
    val input = commandLine.getOptionValue("input")
    val region = commandLine.getOptionValue("region")
    val hour = commandLine.getOptionValue("hour")
    */
    val host = "localhost"
    val database = "wangjf"
    val table = "realtime_service_hour"
    //  val cluster = "test_shard_localhost"
    val input = "/Users/wangjf/Downloads/part-00000-29d98465-8d5d-4325-8d0a-501964f4a21e-c000.zlib.orc"
    val region = "cn"
    val date = "2019-10-15"
    val hour = "10"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RealtimeSerivceJob")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val sc = spark.sparkContext

      import spark.implicits._
      //  val clusterName = Some(cluster): Option[String]
      val clusterName = None
      implicit val clickhouseDataSource: ClickHouseDataSource = ClickHouseConnectionFactory.get(host)

      val partDate = sdf2.format(sdf1.parse(date))

      val dspDF = spark.read.orc(input)
        .rdd
        .map(r => {
          val map = new mutable.HashMap[Int, String]()
          val package_list = r.getAs("package_list").asInstanceOf[mutable.WrappedArray[Int]]
          package_list.foreach(packageId => {
            map.put(packageId, partDate)
          })
          val version = new Date().getTime

          RealtimeServiceHour(r.getAs("device_id"), r.getAs("platform"), 0, 0, r.getAs("country_code"),
            Array.empty[String], mutable.WrappedArray.make(map.keys.toArray), mutable.WrappedArray.make(map.values.toArray), "", version)

          /*
          RealtimeServiceHour(r.getAs("device_id"), r.getAs("platform"), 0, 0, r.getAs("country_code"),
            Array.empty[String], Array.empty[Int], Array.empty[String], "",version)l
          */
        }).toDF
      //  .select(upper(col("device_id")).alias("device_id"),
      //  col("platform"), col("country_code").alias("country"), col("package_list").alias("install_apps"))

      /**
        * user_info save
        */
      //  dspDF.createClickHouseDb(database, clusterName)
      //  dspDF.createClickHouseTable(database, table, Seq("dt", "hour", "region"), Constant.indexColumn, Constant.orderColumn, clusterName)

      //  val emptyDF = spark.emptyDataFrame
      //  emptyDF.dropPartition(database, table, s"($partDate,'$hour','$region')", clusterName)

      dspDF.saveToClickHouse(database, table, Seq(date, hour, region), Seq("dt", "hour", "region"), clusterName, batchSize = 1000000)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object RealtimeSerivceJob {
  def main(args: Array[String]): Unit = {
    new RealtimeSerivceJob().run(args)
  }
}
