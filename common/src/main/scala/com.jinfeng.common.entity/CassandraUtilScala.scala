package com.jinfeng.common.entity

import java.util

import com.alibaba.fastjson.JSONObject
import com.datastax.driver.core.{BoundStatement, PreparedStatement, ResultSet, Row, Session}
import com.jinfeng.common.utils.CassandraConnector
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
 * @package: com.jinfeng.common.entity
 * @author: wangjf
 * @date: 2020/9/22
 * @time: 6:17 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object CassandraUtilScala {
  def main(args: Array[String]): Unit = {

    val sql: String = "select devid,region from dmp.recent_device_region where devid = 'key5'"

    val cassandraConnector = new CassandraConnector()

    val session: Session = cassandraConnector.connect()

    val preparedStatement: PreparedStatement = session.prepare(sql)

    val boundStatement: BoundStatement = preparedStatement.bind

    val result: ResultSet = session.execute(boundStatement)

    /*
    for (Row row : resultSet) {
        System.out.printf("devid: %s, update_time: %s, audience_data: %s\n", row.getString("devid"), row.getString("update_time"), row.getString("audience_data"));
    }
    */

    val regionArray: mutable.WrappedArray[String] = mutable.WrappedArray.make(mutable.Set("cn", "virginia", "seoul", "tokyo", "frankfurt", "singapore").toArray)

    import scala.collection.JavaConversions._
    println("result.nonEmpty -->> " + result.nonEmpty)
    val install_region =
      if (result.nonEmpty) {
        val row = result.one()
        println("result -->> " + row)
        val region = row.getObject("region").asInstanceOf[java.util.Set[String]]
        val install_list = row.getString("devid")
        //  val region = row.getString("region")
        //  println(StringUtils.isNotBlank(dev_type))
        println("region -->> " + region + " ,install_list -->> " + install_list)
        if (region.nonEmpty) {
          (install_list, mutable.WrappedArray.make[String](region.toArray))
        } else {
          (install_list, regionArray)
        }
      } else {
        (new JSONObject().toJSONString, regionArray)
      }
    println("parseQueryRegion -->> " + install_region)
    /*
    val row: Row = resultSet.one

    System.out.println(row)

    val region = row.getObject(1).asInstanceOf[java.util.Set[String]]
    val devid: String = row.getString(0)

    //  System.out.printf("devid: %s, update_time: %s, audience_data: %s\n", row.getString("devid"), row.getObject("region"), row.getObject("region"));
    System.out.printf("devid: %s, update_time: %s, audience_data: %s\n", devid, region, region)
    */

  }
}
