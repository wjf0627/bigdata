package com.jinfeng.flink.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @package: com.jinfeng
  * @author: wangjf
  * @date: 2019/1/8
  * @time: 下午6:49
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {

    // the port to connect to
    val port: Int = try {
      // ParameterTool.fromArgs(args).getInt("port")
      12345
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Int)

}
