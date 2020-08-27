package com.jinfeng.spark.example.streaming

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @package: com.jinfeng.spark.streaming
  * @author: wangjf
  * @date: 2019/1/23
  * @time: 下午7:08
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object KafkaWorldCount {
  def main(args: Array[String]) {

    val args = new Array[String](4)
    args(0) = "localhost:9092"
    args(1) = "consumer-group"
    args(2) = "test1"
    args(3) = "5"
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkSession = SparkSession.builder().master("local[5]").appName("Streaming").getOrCreate()
    val sparkConf = sparkSession.sparkContext

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("src/main/resources/checkpoint/")

    //  val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicSet = topics.split(",")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )

    val words = stream.flatMap(_.value().split(" "))
    val wordCounts = words.map(x => (x, 1))
      .reduceByKeyAndWindow(_ + _, Minutes(1))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

object KafkaWordCountProducer {
  def main(args: Array[String]) {
    val args = new Array[String](4)
    args(0) = "localhost:9092"
    args(1) = "test1"
    args(2) = "10"
    args(3) = "10"
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val props = new util.HashMap[String, Object]
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      (1 to messagesPerSec.toInt).foreach { _ =>
        val str = (1 to wordsPerMessage.toInt).map(_ => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(10)
    }
  }
}
