package com.jinfeng.spark.utils

import org.apache.spark.sql.SparkSession

/**
 * @package: com.jinfeng.spark.utils
 * @author: wangjf
 * @date: 2020/6/10
 * @time: 4:22 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object BigDataSession {

  def createSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "lz4")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/spark-warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
  }
}
