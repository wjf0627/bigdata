package com.jinfeng.spark.example.fmp

import com.google.gson.{JsonArray, JsonObject}
import com.jinfeng.common.utils.GsonUtil
import com.jinfeng.spark.example.entity._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * @package: com.jinfeng.spark.example.fmp
  * @author: wangjf
  * @date: 2019-06-10
  * @time: 17:03
  * @emial: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object ParseJob {

  def parseJsonStringJob(jsonString: String): JobProfileEntity = {
    GsonUtil.fromJson(GsonUtil.String2JsonObject(jsonString), classOf[JobProfileEntity])
  }

  def parseDimension(json: JsonObject): DimensionEntity = {
    GsonUtil.fromJson(json, classOf[DimensionEntity])
  }

  val data =
    """
      |{
      |                "job_id": "06019491-8372-4a28-859d-de0bfcc1773e",
      |                "job_name": "test",
      |                "user_id": "123",
      |                "country": null,
      |                "platform": null,
      |                "package_name": "[\"com.blued.international\",\"www.baidu.com\"]",
      |                "last_req_day": 0,
      |                "dimension": "[\n    [\n        {\n            \"interest\":\"01001001\",\n            \"install_cnt\":\"7-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        },\n        {\n            \"interest\":\"01001002\",\n            \"install_cnt\":\"7-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        }\n    ],\n    [\n        {\n            \"interest\":\"01001004\",\n            \"install_cnt\":\"3-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        }\n    ]\n]",
      |                "behavior": null,
      |                "flag": 0,
      |                "path": 0,
      |                "limit": 0,
      |                "create_time": "2019-06-05T02:26:08.000+0000",
      |                "return_data_num": null
      |            }
    """.stripMargin

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CassandraExample")
      .master("local")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()


    val jobProfileEntity = parseJsonStringJob(data)

    val job_id = jobProfileEntity.job_id

    val package_name = GsonUtil.String2JsonArray(jobProfileEntity.package_name)

    val dimension = GsonUtil.String2JsonArray(jobProfileEntity.dimension)

    //  import scala.collection.JavaConversions._

    //  println("dimension ==>> " + dimensionForeach(dimension))

    var input = "/Users/wangjf/Workspace/data/part-00003.snappy.orc"
    //  import spark.implicits._
    val df = spark.read.orc(input)
      .rdd
      .map(r => {
        DeviceTag(r.getAs("device_id"), r.getAs("age"), r.getAs("gender"), r.getAs("install"), r.getAs("interest"), r.getAs("frenquency"))
      })

    input = "/Users/wangjf/Workspace/data/dm_active_tag.txt"
    val active_df = spark.sparkContext.textFile(input)
      .map(r => r.split(";"))
      .map(r => {
        DeviceActive(r(0), r(1), r(2))
      })
    dimensionForeach(df, active_df, dimension, spark).take(10).foreach(println)

    //  rdd.createOrReplaceTempView("user_info")

    //  Constant.dimensionForeach(df, dimensions, spark).show(20)
  }

  def dimensionForeach(df: RDD[DeviceTag], active_df: RDD[DeviceActive], jsonArray: JsonArray, spark: SparkSession): DataFrame = {
    import spark.implicits._
    var outer_df = spark.emptyDataFrame
    jsonArray.foreach(json => {
      var inter_df = spark.emptyDataFrame
      json.getAsJsonArray.foreach(j => {
        val dimensionEntity = parseDimension(j.getAsJsonObject)
        val interest = dimensionEntity.interest
        val install_cnt = dimensionEntity.install_cnt
        val active_count = dimensionEntity.active_count
        val active_dimension = dimensionEntity.active_dimension
        inter_df = if (inter_df.rdd.isEmpty()) {
          parseInterestCnt(df, interest, install_cnt).toDF.intersect(parseInterestActive(active_df, interest, active_count, active_dimension).toDF)
        } else {
          inter_df.union(parseInterestCnt(df, interest, install_cnt).toDF.intersect(parseInterestActive(active_df, interest, active_count, active_dimension).toDF))
        }
      })
      outer_df = if (outer_df.rdd.isEmpty()) {
        inter_df
      } else {
        outer_df.intersect(inter_df)
      }
    })
    outer_df
  }

  def parseInterestCnt(df: RDD[DeviceTag], interest: String, install_cnt: String): RDD[String] = {
    val lower = Integer.parseInt(install_cnt.split("-")(0))
    val biger = Integer.parseInt(install_cnt.split("-")(1))
    val result = df.filter(d => {
      var flag = false
      if (d.frequency != null) {
        for (i <- d.frequency.indices if !flag) {
          val tag = d.frequency.get(i).asInstanceOf[GenericRowWithSchema].getAs("tag").toString
          val cnt = Integer.parseInt(d.frequency.get(i).asInstanceOf[GenericRowWithSchema].getAs("cnt").toString)
          flag = tag.equals(interest) && cnt >= lower && cnt <= biger
        }
      }
      flag
    }).map(r => {
      r.device_id
    })
    result
  }

  def parseInterestActive(df: RDD[DeviceActive], interest: String, active_count: String, active_dimension: String): RDD[String] = {
    val lower = Integer.parseInt(active_count.split("-")(0))
    val biger = Integer.parseInt(active_count.split("-")(1))
    val result = df.filter(d => {
      var flag = false
      val tags = GsonUtil.String2JsonArray(d.tag)
      for (tag <- tags if !flag) {
        val active = GsonUtil.fromJson(tag.getAsJsonObject, classOf[ActiveTag])
        val tag_id = active.tag_id
        val cnt = active.cnt
        flag = tag_id.equals(interest) && cnt >= lower && cnt <= biger
      }
      flag
    }).map(r => {
      r.device_id
    })
    result
  }

  val dimension = "[\n    [\n        {\n            \"interest\":\"01001001\",\n            \"install_cnt\":\"7-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        },\n        {\n            \"interest\":\"01001002\",\n            \"install_cnt\":\"7-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        }\n    ],\n    [\n        {\n            \"interest\":\"01001003\",\n            \"install_cnt\":\"7-9\",\n            \"active_count\":\"3-4\",\n            \"active_dimension\":\"week\"\n        }\n    ]\n]"
}
