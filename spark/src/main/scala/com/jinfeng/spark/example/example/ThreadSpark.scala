package com.jinfeng.spark.example.example

import java.util.concurrent.{ExecutorService, Executors}

import org.apache.spark.sql.SparkSession

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2019-09-06
 * @time: 15:13
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object ThreadSpark {

  def main(args: Array[String]): Unit = {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(1)
    try {
      //  提交5个线程
      for (i <- 0 to 5) {
        threadPool.submit(new ThreadSpark("thread" + i))
      }
    } finally {
      threadPool.shutdown()
    }
  }
}

class ThreadSpark(threadDemo: String) extends Runnable {

  @Override
  def run() {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("ThreadSpark")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    //  val conf = new SparkConf().setAppName(threadDemo)
    //  conf.set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val sc = spark.sparkContext
    println("AppName === " + threadDemo + ",ApplicationId === " + sc.applicationId)
    val rdd = sc.parallelize(List((1, 2), (3, 4), (3, 6)))
    rdd.foreach(println)
    sc.stop()
  }
}