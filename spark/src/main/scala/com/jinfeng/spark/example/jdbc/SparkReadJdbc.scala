package com.jinfeng.spark.example.jdbc

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql._

/**
  * @package: com.jinfeng.spark.example.jdbc
  * @author: wangjf
  * @date: 2019-06-06
  * @time: 18:17
  * @emial: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
case class Result(id: Int, app_package_name: String, ctime: Timestamp, mtime: Timestamp) extends java.io.Serializable

object SparkReadJdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("GetPackage")
      .config("spark.rdd.compress", "true")
      .config("spark.shuffle.compress", "true")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.io.compression.lz4.blockSize", "64k")
      .config("spark.sql.autoBroadcastJoinThreshold", "209715200")
      .config("spark.sql.warehouse.dir", "s3://mob-emr-test/spark-warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    import spark.implicits._

    val mysqlDF = jdbcConnection(spark, "mob_adn", "dmp_app_map").rdd.map(r => {
      r.getAs("app_package_name").toString
    }).toDF("app_package_name")
    //  println(mysqlDF.limit(20))

    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sc = spark.sparkContext

    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.HOUR, 13)

    val df = sc.parallelize(List("www.baidu.com", "www.wangjf.com")).toDF("app_package_name")
      .except(mysqlDF)
      .map(r => {
        Result(0, r.getAs("app_package_name"), Timestamp.valueOf(fm.format(cal.getTime)), Timestamp.valueOf(fm.format(cal.getTime)))
        //  r
      }).toDF

    writeMySQL(df, "mob_adn", "dmp_app_map", SaveMode.Append)

  }

  def jdbcConnection(spark: SparkSession, database: String, table: String): DataFrame = {
    val properties = new Properties()
    properties.put("driver", "com.mysql.cj.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "19920627")
    //  properties.put("user", "apptag_rw")
    //  properties.put("password", "7gyLEVtkER3u8c9")
    //  val url = s"jdbc:mysql://dataplatform-app-tag.c5yzcdreb1xr.us-east-1.rds.amazonaws.com:3306/${database}"
    val url = s"jdbc:mysql://localhost:3306/${database}?useUnicode=true&characterEncoding=utf8&useSSL=false"

    spark.read.jdbc(url = url, table = table, properties = properties)
  }

  def writeMySQL(df: Dataset[Row], database: String, table: String, saveMode: SaveMode): Unit = {
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", "root")
    properties.put("password", "19920627")
    //  properties.put("user", "apptag_rw")
    //  properties.put("password", "7gyLEVtkER3u8c9")
    properties.put("characterEncoding", "utf8")
    //  val url = s"jdbc:mysql://dataplatform-app-tag.c5yzcdreb1xr.us-east-1.rds.amazonaws.com:3306/${database}"
    val url = s"jdbc:mysql://localhost:3306/${database}?useUnicode=true&characterEncoding=utf8&useSSL=false"

    df.write.mode(saveMode).jdbc(url, table, properties)
  }
}
