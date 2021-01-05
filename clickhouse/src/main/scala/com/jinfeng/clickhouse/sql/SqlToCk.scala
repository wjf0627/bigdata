package com.jinfeng.clickhouse.sql

import java.util.UUID

import com.jinfeng.clickhouse.DeviceA
//  import com.jinfeng.clickhouse.util.ClickHouseConnectionFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @package: com.jinfeng.clickhouse.sql
  * @author: wangjf
  * @date: 2019/5/24
  * @time: 下午5:24
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object SqlToCk {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlToCk")
      .master("local[10]")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    try {

      val sc = spark.sparkContext
      //  val host = "localhost"
      val db = "wangjf"
      val tableName = "device_set"
      //  val clusterName = None
      //  implicit val clickhouseDataSource = ClickHouseConnectionFactory.get(host)
      import spark.implicits._
      val df = sc.parallelize(1 to 100).map(f = id => {
        DeviceA("" + UUID.randomUUID(), Array(0),"")
      }).toDF

      //  val connection = clickhouseDataSource.getConnection
      //  val createSql = "CREATE TABLE wangjf.device_set(device_id String, offer_id Array(Int32)) ENGINE = TinyLog;"
      //  connection.createStatement.execute (createSql)
      //  import ru.yandex.clickhouse.ClickHousePreparedStatement

      //  val insertSQL = "INSERT INTO wangjf.device_set(device_id, offer_id) VALUES (?,?)"
      //  val statement = connection.prepareStatement (insertSQL).unwrap (classOf [ClickHousePreparedStatement])
      //  statement.executeUpdate

      val prop = new java.util.Properties
      prop.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver")

      //  val table = "device_set"
      //  df.write.mode (SaveMode.Overwrite).jdbc ("jdbc:clickhouse://localhost:8123/wangjf", table, prop)
      //  val options: java.util.Map[String, String] = new util.HashMap[String, String]()
      //  options.put ("driver", "ru.yandex.clickhouse.ClickHouseDrive")
      //  options.put ("url", "jdbc:clickhouse://localhost:8123/wangjf")
      df.write.mode(SaveMode.Overwrite).jdbc(s"jdbc:clickhouse://localhost:8123/${db}", tableName, prop)

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
