package com.jinfeng.spark.example.streaming

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @package: com.jinfeng.spark.streaming
  * @author: wangjf
  * @date: 2019/1/23
  * @time: 下午6:31
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object Streaming {

  def main(args: Array[String]): Unit = {

    val args = new Array[String](4)
    args(0) = "localhost:9092"
    args(1) = "consumer-group"
    args(2) = "metric"
    args(3) = "5"
    if (args.length < 4) {
      System.err.println("Usage: Streaming <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args

    val spark = SparkSession.builder().master("local").appName("Streaming").getOrCreate()
    val sparkConf = spark.sparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("src/main/resources/checkpoint/")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicSet = topics.split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    val words = stream.map(r => {
      val json = JSON.parseObject(r.value())
      (json.getJSONObject("fields").getString("init"), json.getJSONObject("fields").getInteger("used"))
    })

    val wordCounts = words.reduceByKeyAndWindow(_ + _, Seconds(10))
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    /*
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    ssc.checkpoint("src/main/resources/checkpoint/")
    val words = lines.map(r => {
      val json = JSON.parseObject(r)
      (json.getJSONObject("fields").getString("init"), json.getJSONObject("fields").getString("used"))
    })
    val wordCounts = words.reduceByKeyAndWindow(_ + _, Minutes(1))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    */
  }
}
