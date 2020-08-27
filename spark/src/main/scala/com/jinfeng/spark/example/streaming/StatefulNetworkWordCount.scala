package com.jinfeng.spark.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * @package: com.jinfeng.spark.streaming
  * @author: wangjf
  * @date: 2019/3/22
  * @time: 下午6:39
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object StatefulNetworkWordCount {


  def main(args: Array[String]) {
    val args = new Array[String](2)
    args(0) = "127.0.0.1"
    args(1) = "7777"
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val spark = SparkSession.builder().master("local[3]").appName("Streaming").getOrCreate()
    val sparkConf = spark.sparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")
    //  Initial state RDD for mapWithState operation
    val initialRDD = ssc.sparkContext.parallelize(List(("Hello", 1), ("World", 1)))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val windowedWordCount = wordDstream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(2), Seconds(1))

    //  Update the cumulative count using mapWithState
    //  This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }

    // CREATE TABLE example.words (word text PRIMARY KEY, count int);

    val stateDstream = windowedWordCount.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD)
    )
    stateDstream.print()
    import com.datastax.spark.connector.streaming._
    //    stateDstream.saveToCassandra("example", "words")
    windowedWordCount.saveToCassandra("streaming_test", "words")
    ssc.start()
    ssc.awaitTermination()
  }
}
