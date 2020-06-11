package com.jinfeng.spark.sql

import com.alibaba.fastjson.{JSON, JSONObject}
import com.jinfeng.spark.constant.Constant
import com.jinfeng.spark.utils.BigDataSession
import org.apache.hadoop.fs.{FileSystem, Path}
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

      val sql1 =
        """
          |SELECT * FROM dwh.t1
          |""".stripMargin

      val sql_df = spark.sql(sql1).toDF()

      FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(output + "/sql_df"), true)
      sql_df.write.mode(SaveMode.Overwrite).orc(output + "/sql_df")
      /*
      val sql2 =
        """
          |SELECT * FROM dwh.t5
          |""".stripMargin

      spark.sql(sql1).union(spark.sql(sql2))
        .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwh.t1")
      */
      FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new Path(output + "/df"), true)
      df.write.mode(SaveMode.Overwrite).orc(output + "/df")

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