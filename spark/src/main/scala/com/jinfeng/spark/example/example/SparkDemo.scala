package com.jinfeng.spark.example.example

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2019-09-11
 * @time: 17:42
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */

/**
 * @author wangjf
 */

case class User(dev_id: String, tag: String, data: mutable.Map[String, String]) extends java.io.Serializable

object SparkDemo {
  /*
  private val wellSplit = Pattern.compile("#")
  private val colonSplit = Pattern.compile(":")
  private val verticalLine = Pattern.compile("\\|")
  */

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkTest")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc = spark.sparkContext
    try {

      /*
      val map = new mutable.HashMap[Int, (String, Int)]()
      map.put(123, ("/Users/wangjf/Workspace/data/device_id.txt", 1))
      //  map.put(456, ("/Users/wangjf/Workspace/data/device_id.txt", 1))
      //  map.put(789, ("/Users/wangjf/Workspace/data/device_id.txt", 1))
      var rdd = sc.emptyRDD[(String, String, Int)]
      map.foreach(m => {
        rdd = rdd.union(sc.textFile(m._2._1).map(r => {
          val array = new ArrayBuffer[(String, String, Int)]()
          if (StringUtils.isNotBlank(r) && (r.length == 32 || r.length == 31 || r.length == 30)) {
            array += ((r, r, r.length))
          } else {
            array += ((r, MD5Util.getMD5Str(r.toLowerCase()), MD5Util.getMD5Str(r.toLowerCase()).length))
            array += ((r, MD5Util.getMD5Str(r.toUpperCase()), MD5Util.getMD5Str(r.toUpperCase()).length))
          }
          array
        }).flatMap(l => l)
        )
      })
      val output = "/Users/wangjf/Workspace/data/device"
      FileSystem.get(sc.hadoopConfiguration).delete(new Path(output), true)
      rdd.saveAsTextFile(output)
      */
      import spark.implicits._
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
      val df = sc.parallelize(set.toList).map(r => {
        import scala.collection.JavaConversions._
        val map = new mutable.HashMap[String, String]()
        JSON.parseArray(r._3).foreach(j => {
          val json = j.asInstanceOf[JSONObject]
          map.put(json.getString("package_name"), json.getString("date"))
        })
        User(r._1, r._2, map)
      }).toDF

      val schema = ScalaReflection.schemaFor[Tracking3S].dataType.asInstanceOf[StructType]
      println("schema --->>> " + schema)
      //  df.printSchema()
      df.take(10).foreach(println)
      /*
      df.select(col("dev_id"), getPkgSize(col("data")).alias("size"))
        .select(count(lit(1)).alias("uv"), sum(col("size")).alias("pv"))
        .show(10)
      */
      /*
      val newdf = df.groupBy("dev_id")
        .agg(
          collect_count(collect_list("tag")),
          max("dt"))
        .toDF("dev_id", "tags", "dt")

      newdf.show(10)
      */
      //  newdf.printSchema()

    } finally {
      sc.stop()
      spark.stop()
    }


  }

  val getPkgSize = udf((tags: String) => {
    val set: mutable.Set[String] = new mutable.HashSet[String]()
    import scala.collection.JavaConversions._
    JSON.parseArray(tags).foreach(json => {
      if (json.asInstanceOf[JSONObject].keySet().contains("package_name")) {
        set.add(json.asInstanceOf[JSONObject].getString("package_name").toLowerCase)
      }
    })
    set.size
  })

  def collect_count = udf((tags: mutable.WrappedArray[String]) => {
    //  val map = new mutable.HashMap[String, Int]()
    val json = new JSONObject()
    tags.foreach(t => {
      /*
      if (map.keySet.contains(t)) {
        map.put(t, map(t) + 1)
      } else {
        map.put(t, 1)
      }
      */
      if (json.containsKey(t)) {
        json.put(t, Integer.parseInt(json.get(t).toString) + 1)
      } else {
        json.put(t, 1)
      }
    })
    json.toJSONString
  })
}

