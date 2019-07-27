package com.jinfeng.clickhouse.util

import java.sql.ResultSet

import com.jinfeng.clickhouse.util.Utils._
import ru.yandex.clickhouse.ClickHouseDataSource

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019/5/20
  * @time: 下午3:45
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */

case class ClickHouseClient(clusterNameO: Option[String] = None)
                           (implicit ds: ClickHouseDataSource) {

  import com.jinfeng.clickhouse.util.ClickHouseResultSetExt._

  def createDb(dbName: String) {
    query(s"create database if not exists $dbName")
  }

  def dropDb(dbName: String) {
    query(s"DROP DATABASE IF EXISTS $dbName")
  }

  def query(sql: String) = {
    using(ds.getConnection) { conn =>
      val statement = conn.createStatement()
      val rs = statement.executeQuery(sql)
      rs
    }
  }

  def queryCluster(sql: String): ClusterResultSet = {
    val resultSet = runOnAllNodes(sql)
    ClusterResultSet(resultSet)
  }

  def createDbCluster(dbName: String): Int = {
    runOnAllNodes(s"create database if not exists $dbName")
      .count(x => x._2 == null)
  }

  def dropDbCluster(dbName: String): Int = {
    runOnAllNodes(s"DROP DATABASE IF EXISTS $dbName")
      .count(x => x._2 == null)
  }

  def getClusterNodes(): Seq[String] = {
    val clusterName = isClusterNameProvided()
    using(ds.getConnection) { conn =>
      val statement = conn.createStatement()
      val rs = statement.executeQuery(s"select host_name, host_address from system.clusters where cluster == '$clusterName'")
      val r = rs.map(x => x.getString("host_name"))
      require(r.nonEmpty, s"cluster $clusterNameO not found")
      r
    }
  }

  private def runOnAllNodes(sql: String): Seq[(String, ResultSet)] = {
    getClusterNodes().map { nodeIp =>
      val nodeDs = ClickHouseConnectionFactory.get(nodeIp)
      val client = ClickHouseClient()(nodeDs)
      (nodeIp, client.query(sql))
    }
  }

  private def isClusterNameProvided(): String = {
    clusterNameO match {
      case None => throw new Exception("cluster name is requires")
      case Some(clusterName) => clusterName
    }
  }
}