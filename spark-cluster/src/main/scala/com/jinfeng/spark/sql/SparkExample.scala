package com.jinfeng.spark.sql

import com.alibaba.fastjson.{JSON, JSONObject}
import com.jinfeng.spark.constant.Constant
import com.jinfeng.spark.utils.BigDataSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SaveMode

import scala.collection.mutable

/**
 * @package: com.jinfeng.spark.sql
 * @author: wangjf
 * @date: 2020/6/10
 * @time: 4:26 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
class SparkExample {
  def run(args: Array[String]): Unit = {
    val spark = BigDataSession.createSparkSession("SparkExample")

    try {

      val set = new mutable.HashSet[(String, String, String)]()
      set.add(("A", "a",
        """
          |[{"date":"2020-05-08","package_name":"791532221","tag":[{"1":"Games","2":"other","id":"69"},{"1":"Games","2":"Party Games","id":"78"},{"1":"Entertainment","2":"other","id":"66"},{"1":"Games","2":"Puzzle","id":"80"}],"tag_new":[{"id":"","1":"moderategame"},{"id":"79","1":"elimination"}]},{"date":"2019-12-17","package_name":"1474052323","tag":[{"1":"Health & Fitness","2":"other","id":"88"},{"1":"Lifestyle","2":"other","id":"89"}]},{"date":"2020-01-06","package_name":"393765873","tag":[{"1":"Tools","2":"other","id":"146"},{"1":"Entertainment","2":"other","id":"66"}],"tag_new":[{"id":"159","1":"Video","2":"Online Video"}]},{"date":"2020-05-05","package_name":"433156786","tag":[{"1":"Business","2":"other","id":"124"},{"1":"Social","2":"Social Networks","id":"127"}],"tag_new":[{"id":"150","1":"Social","2":"other"}]},{"date":"2020-04-30","package_name":"399363156","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"News & Magazines","2":"News","id":"122"}],"tag_new":[{"id":"170","1":"News & Magazines","2":"News"}]},{"date":"2020-05-01","package_name":"416048305","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"Photography","id":"123"}],"tag_new":[{"id":"133","1":"Tools","2":"other"},{"id":"","1":"Tools"}]}]
          |""".stripMargin))
      set.add(("B", "a",
        """
          |[{"date":"2020-05-08","package_name":"791532221","tag":[{"1":"Games","2":"other","id":"69"},{"1":"Games","2":"Party Games","id":"78"},{"1":"Entertainment","2":"other","id":"66"},{"1":"Games","2":"Puzzle","id":"80"}],"tag_new":[{"id":"","1":"moderategame"},{"id":"79","1":"elimination"}]},{"date":"2019-12-17","package_name":"1474052323","tag":[{"1":"Health & Fitness","2":"other","id":"88"},{"1":"Lifestyle","2":"other","id":"89"}]},{"date":"2020-01-06","package_name":"393765873","tag":[{"1":"Tools","2":"other","id":"146"},{"1":"Entertainment","2":"other","id":"66"}],"tag_new":[{"id":"159","1":"Video","2":"Online Video"}]},{"date":"2020-05-05","package_name":"433156786","tag":[{"1":"Business","2":"other","id":"124"},{"1":"Social","2":"Social Networks","id":"127"}],"tag_new":[{"id":"150","1":"Social","2":"other"}]},{"date":"2020-04-30","package_name":"399363156","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"News & Magazines","2":"News","id":"122"}],"tag_new":[{"id":"170","1":"News & Magazines","2":"News"}]},{"date":"2020-05-01","package_name":"416048305","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"Photography","id":"123"}],"tag_new":[{"id":"133","1":"Tools","2":"other"},{"id":"","1":"Tools"}]}]
          |""".stripMargin))
      set.add(("C", "c",
        """
          |[{"date":"2020-05-08","package_name":"791532221","tag":[{"1":"Games","2":"other","id":"69"},{"1":"Games","2":"Party Games","id":"78"},{"1":"Entertainment","2":"other","id":"66"},{"1":"Games","2":"Puzzle","id":"80"}],"tag_new":[{"id":"","1":"moderategame"},{"id":"79","1":"elimination"}]},{"date":"2019-12-17","package_name":"1474052323","tag":[{"1":"Health & Fitness","2":"other","id":"88"},{"1":"Lifestyle","2":"other","id":"89"}]},{"date":"2020-01-06","package_name":"393765873","tag":[{"1":"Tools","2":"other","id":"146"},{"1":"Entertainment","2":"other","id":"66"}],"tag_new":[{"id":"159","1":"Video","2":"Online Video"}]},{"date":"2020-05-05","package_name":"433156786","tag":[{"1":"Business","2":"other","id":"124"},{"1":"Social","2":"Social Networks","id":"127"}],"tag_new":[{"id":"150","1":"Social","2":"other"}]},{"date":"2020-04-30","package_name":"399363156","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"News & Magazines","2":"News","id":"122"}],"tag_new":[{"id":"170","1":"News & Magazines","2":"News"}]},{"date":"2020-05-01","package_name":"416048305","tag":[{"1":"Social","2":"Social Networks","id":"127"},{"1":"Photography","id":"123"}],"tag_new":[{"id":"133","1":"Tools","2":"other"},{"id":"","1":"Tools"}]}]
          |""".stripMargin))

      import spark.implicits._
      import scala.collection.JavaConversions._
      val df = spark.sparkContext.parallelize(set.toList).map(r => {
        val jsonObject = new JSONObject()
        JSON.parseArray(r._3).foreach(j => {
          val json = j.asInstanceOf[JSONObject]
          jsonObject.put(json.getString("package_name"), json.getString("date"))
        })
        Constant.User(r._1, r._2, jsonObject.toJSONString)
      }).toDF


      val output = "/user/wangjf/data/spark_example"

      //  FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(output), true)


      val sql2 =
        """
          |SELECT t.id id,t.name name, m.score score FROM dwh.t1 t
          | JOIN dwh.t2 m ON t.id = m.id
          |""".stripMargin

      val sql1 =
        """
          |SELECT * FROM dwh.t3
          |""".stripMargin
      val sql_df = spark.sql(sql1)

      //  sql_df.write.mode(SaveMode.Overwrite).insertInto("dwh.t3")
      //  val sql_df_1 = spark.read.orc(output + "/sql_df")
      FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(output + "/txt"), true)
      sql_df.write.mode(SaveMode.Overwrite).insertInto("dwh.t6")
      //  .coalesce(1).rdd.saveAsTextFile(output + "/txt", classOf[GzipCodec])
      //  .write.mode(SaveMode.Overwrite).parquet(output + "/parquet")

      //  .mode(SaveMode.Overwrite).insertInto("dwh.t6")
      val sql3 =
        """
          |SELECT * FROM dwh.t5
          |""".stripMargin

      //  spark.sql(sql1).union(spark.sql(sql3))
      //  .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwh.t1")

      FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(output + "/parquet_1"), true)
      spark.sql(sql3).write.mode(SaveMode.Overwrite).parquet(output + "/parquet_1")

      //  Thread.sleep(120000)
    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}

object SparkExample {

  def main(args: Array[String]): Unit = {
    new SparkExample().run(args)
  }
}