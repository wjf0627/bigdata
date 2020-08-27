package com.jinfeng.flink.cassandra

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**
  * @package: com.jinfeng
  * @author: wangjf
  * @date: 2019/1/8
  * @time: 下午6:49
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object FlinkCassandraExample {

  def main(args: Array[String]): Unit = {

    // the port to connect to
    val port: Int = try {
      // ParameterTool.fromArgs(args).getInt("port")
      12345
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'FlinkCassandraExample --port <port>'")
        return
      }
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    val result: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)

    //  print the results with a single thread, rather than in parallel
    result.print().setParallelism(1)

    CassandraSink.addSink(result)
      .setQuery("INSERT INTO example.words(word, count) values (?, ?);")
      .setHost("127.0.0.1")
      .build()

    env.execute("Flink Cassandra WordCount")
  }

  // Data type for words with count
}
