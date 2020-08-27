package com.jinfeng.spark.example.example

import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2019-09-23
 * @time: 14:50
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("SparkTestA")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    try {

      /*
      val input = "/Users/wangjf/Workspace/data/part-00003.snappy.orc"

      val rdd = spark.read.orc(input)
      rdd.show()
      rdd.rdd.filter(r => {
        r.getAs("frenquency") != null
      }).map(r => {
        val deviceFreq = DeviceFreq(r.getAs("frenquency"))
        val frequencys = Array.empty[StructEntity]
        if (deviceFreq.frenquency != null) {
          val iter = deviceFreq.frenquency
          for (i <- iter.indices) {
            val tag = iter(i).asInstanceOf[GenericRowWithSchema].getAs("tag").toString
            val cnt = Integer.parseInt(iter(i).asInstanceOf[GenericRowWithSchema].getAs("cnt").toString)
            frequencys.+:(StructEntity(tag, cnt))
          }
        }
        frequencys.length
      }).take(10).foreach(println)
      */
      import spark.implicits._
      /*
      val df = sc.parallelize(Seq(
        DmpInterest("ABC",
          """{"m":{"dev_tag":1,"strategy":["MNormalAlphaModelRankerHH","MNormalAlphaModelRankerMM"],"region":["virginia","cn"],"last_date":"2020-04-25"},
            |"dsp":{"dev_tag":1,"region":["virginia","cn"],"last_date":"2020-04-24"}
            |}""".stripMargin),
        DmpInterest("ABD",
          """{"m":{"dev_tag":1,"strategy":["MNormalAlphaModelRankerHH","MNormalAlphaModelRankerMM"],"region":["virginia","cn"],"last_date":"2020-04-25"}}""".stripMargin),
        DmpInterest("ABE",
          """{"dsp":{"dev_tag":1,"region":["virginia","cn"],"last_date":"2020-04-24"}}""".stripMargin),
        DmpInterest("ABF",
          """{"dsp":{"dev_tag":0,"region":["virginia","cn"],"last_date":"2020-04-25"},
            |"m":{"dev_tag":1,"strategy":["MNormalAlphaModelRankerHH","MNormalAlphaModelRankerMM"],"last_date":"2020-04-25"}}""".stripMargin),
        DmpInterest("ABG", "")
      )).toDF()
      */

      def jdbcConnection(spark: SparkSession, database: String, table: String, url: String, user: String, password: String): DataFrame = {
        val properties = new Properties()
        properties.put("driver", "com.mysql.jdbc.Driver")
        properties.put("user", user)
        properties.put("password", password)
        properties.put("characterEncoding", "utf8")

        spark.read.jdbc(url = s"${url}/${database}", table = table, properties = properties)
      }

      val df1 = jdbcConnection(spark, "db", "tb1", "", "", "")
        .select(
          col("column1"),
          col("column2")
        )

      val df2 = jdbcConnection(spark, "db", "tb1", "", "", "")
        .select(
          col("column1"),
          testUDF(col("column2"))
        )
      df1.union(df2)
        .write
        .mode(SaveMode.Overwrite)
        .option("orc.compress", "snappy")
        .orc("output")

      val df = sc.parallelize(Seq(
        DmpInterest("ABF",
          """{"dsp":{"dev_tag":0,"region":["virginia","cn"],"last_date":"2020-04-25"},
            |"m":{"dev_tag":1,"strategy":["MNormalAlphaModelRankerHH","MNormalAlphaModelRankerMM"],"region":[],"last_date":"2020-04-25"}}""".stripMargin),
        DmpInterest("ABG",
          """
            |{"m":{"dev_tag":1,"strategy":["MNormalAlphaModelRankerHH","MNormalAlphaModelRankerMM"],"region":[""],"last_date":"2020-04-25"}}
            |""".stripMargin)
      )).toDF()
      val dff = df.select(col("device_id"), get_json_object(col("ext_data"), "$.m.dev_tag").as("m_dev_tag"),
        get_json_object(col("ext_data"), "$.m.region").as("m_region_list"),
        get_json_object(col("ext_data"), "$.m.strategy").as("m_strategy"),
        get_json_object(col("ext_data"), "$.m.last_date").as("m_update_time"),
        get_json_object(col("ext_data"), "$.dsp.dev_tag").as("dsp_dev_tag"),
        get_json_object(col("ext_data"), "$.dsp.last_date").as("dsp_update_time")
      ).withColumn("region", explode(regionStr2Arr(col("m_region_list"),
        checkMFlag(col("m_dev_tag"), col("m_strategy"), col("m_region_list")),
        checkDSPFlag(col("dsp_dev_tag"))))
      ).withColumn("system", mergeSystem(
        col("region"),
        col("m_region_list"),
        checkMFlag(col("m_dev_tag"), col("m_strategy"), col("m_region_list")),
        checkDSPFlag(col("dsp_dev_tag"))))
        .select(col("device_id"), col("region"), col("system"))

      //  dff.dropDuplicates("device_id")
      dff.show()
    } finally {
      sc.stop()
      spark.stop()
    }
  }

  val testUDF = udf((column: String) => {
    column.size
  })

  val checkDSPFlag = udf((dsp_dev_tag: String) => {
    var flag = false
    if (StringUtils.isNotBlank(dsp_dev_tag)) flag = true
    flag
  })

  val checkMFlag = udf((m_dev_tag: String, m_strategy: String, m_region_list: String) => {
    var isMstrategy = false
    if (m_strategy != null && m_dev_tag.equals("1") && mRegionProcess(m_region_list)) {
      val strategyList = JSON.parseArray(m_strategy).toArray
      for (strategy <- strategyList if strategy.toString.contains("MNormalAlphaModelRanker")) {
        isMstrategy = true
      }
    }
    isMstrategy
  })

  val mRegionProcess = udf((m_region_list: String) => {
    var flag = false
    if (StringUtils.isNotBlank(m_region_list)) {
      val set = new mutable.HashSet[String]()
      val regionArr = JSON.parseArray(m_region_list).toArray()
      for (region <- regionArr) {
        set.add(region.toString)
      }
      if (set.nonEmpty) {
        flag = true
      }
    }
    flag
  })

  def mRegionProcess(m_region_list: String) = {
    var flag = false
    if (StringUtils.isNotBlank(m_region_list)) {
      val set = new mutable.HashSet[String]()
      val regionArr = JSON.parseArray(m_region_list).toArray()
      for (region <- regionArr) {
        set.add(region.toString)
      }
      if (set.nonEmpty) {
        flag = true
      }
    }
    flag
  }

  val regionStr2Arr = udf((mRegion: String, mFlag: Boolean, dspFlag: Boolean) => {
    val res = ArrayBuffer[String]()
    if (mFlag) {
      val regionArr = JSON.parseArray(mRegion)
      for (region <- regionArr) {
        if (StringUtils.isNotBlank(region.toString)) {
          res.append(region.toString)
        }
      }
    } else if (dspFlag) res.append("")
    res.toArray
  })

  val mergeSystem = udf((mRegion: String, m_region_list: String, mFlag: Boolean, dspFlag: Boolean) => {
    val regionRes = new mutable.HashSet[String]()
    if (mFlag) {
      val regionArr = JSON.parseArray(m_region_list).toArray()
      for (region <- regionArr) {
        if (StringUtils.isNotBlank(region.toString)) {
          regionRes.add(region.toString)
        }
      }
    }

    val res = ArrayBuffer[String]()
    if (mFlag) res.append("M")
    if (dspFlag && (!mFlag || (mFlag && regionRes.nonEmpty && util.Arrays.asList(regionRes)(0).equals(mRegion)))) res.append("DSP")
    res.mkString(",")
  })
}

case class DmpInterest(device_id: String, ext_data: String)