package com.jinfeng.flink.example

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * @package: com.jinfeng.flink
  * @author: wangjf
  * @date: 2019/1/23
  * @time: 下午4:06
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object ReadingFromKafka {

  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "flink"

  def main(args: Array[String]) {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //  configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //  topic 的名字是 metric，schema默认使用SimpleStringSchema()即可
    val kafkaConsumer = new FlinkKafkaConsumer[String]("metric", new SimpleStringSchema(), kafkaProps)
    val transaction = env.addSource(kafkaConsumer)
      //  addSource(kafkaConsumer)
    transaction.print()

    env.execute()

  }

}
