package com.jinfeng.clickhouse

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
 * @package: com.jinfeng.clickhouse
 * @author: wangjf
 * @date: 2020/8/17
 * @time: 2:38 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("ThreadSpark")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    val sc = spark.sparkContext

    /*
    val input = "/Users/wangjf/Workspace/data/device/"
    val rdd = sc.textFile(input)

    val boolean = FileSystem.get(sc.hadoopConfiguration).isDirectory(new Path(input))

    println(boolean)
    rdd.take(10).foreach(println)
    */
    val jsonExample =
      """
        |{"devid":"2cf37379cc468b54818949d0fe56ef06","audience_id":[15,12,9,19,16,18,7,11,8]}
        |""".stripMargin
    val json = JSON.parseObject(jsonExample)
    val audience_id = json.getJSONArray("audience_id")

    //  import scala.collection.JavaConverters._
    println(mutable.WrappedArray.make(JSON.parseArray(audience_id.toJSONString,classOf[Int]).toArray()))
    sc.stop()
  }
}
