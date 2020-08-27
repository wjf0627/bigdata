package com.jinfeng.spark.example.sql

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.sql.SparkSession

/**
  * @package: com.jinfeng.spark.example.sql
  * @author: wangjf
  * @date: 2019/5/21
  * @time: 下午3:04
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */

class SparkJdbc {
  def commandOptions(): Options = {
    val options = new Options()
    options.addOption("data", true, "data")
    options
  }

  def run(args: Array[String]): Unit = {

    val parser = new BasicParser()
    val options = commandOptions()
    val commandLine = parser.parse(options, args)
    val data = commandLine.getOptionValue("data")

    val spark = SparkSession
      .builder()
      .appName("SparkJdbc")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    try {
      //  代码

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object SparkJdbc {
  def main(args: Array[String]): Unit = {
    new SparkJdbc().run(args)
  }
}