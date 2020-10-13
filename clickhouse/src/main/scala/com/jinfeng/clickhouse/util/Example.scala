package com.jinfeng.clickhouse.util

import scala.collection.mutable

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019-10-18
  * @time: 14:45
  * @email: jinfeng.wang@mobvista.com
  * @phone: 152-1062-7698
  */
object Example {

  def main(args: Array[String]): Unit = {
    //  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //  println(new Date().getTime)
    //  val set = new mutable.HashSet[(String, Int)]()
    //  val json = new JSONObject()
    val map = new mutable.HashMap[Int, String]()
    //  set.add(("20191013", 123))
    //  set.add(("20191013", 124))
    //  set.add(("20191013", 125))
    //  set.add(("20191013", 126))
    //  println(set.)
    //  json.put("123","20191013")
    //  json.put("124","20191012")
    //  json.put("125","20191011")
    //  json.put("126","20191013")
    //  println(json.keySet())
    //  println(json.values())

    map.put(125, "20191013")
    map.put(124, "20191012")
    map.put(123, "20191011")
    map.put(126, "20191010")
    println(map.keys)
    println(map.values)

    println(mutable.WrappedArray.make(map.toArray))
    println(map.mkString(":").split(":"))
  }
}
