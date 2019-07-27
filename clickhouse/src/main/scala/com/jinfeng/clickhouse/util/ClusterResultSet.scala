package com.jinfeng.clickhouse.util

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019/5/20
  * @time: 下午3:48
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
case class ClusterResultSet(clusterRs: Seq[(String, java.sql.ResultSet)]) {

  import com.jinfeng.clickhouse.util.ClickHouseResultSetExt._

  def get = clusterRs

  def toTab = {
    val firstRow = clusterRs.head
    val firstRowRs = firstRow._2

    val metaTab = if (firstRowRs != null) {
      val meta = firstRowRs.getMeta
      ("host" :: meta.map(x => s"${x._2}").toList).mkString("\t")
    } else {
      Seq("host", "result").mkString("\t")
    }

    val bodyTab = clusterRs.map { cur =>
      val hostIp = cur._1
      if (cur._2 != null) {
        val ds = cur._2.getData // Seq[Seq[AnyRef]]
        ds.map { row =>
          (hostIp :: row.map(v => s"$v").toList).mkString("\t")
        }.mkString("\n")
      } else {
        Seq(hostIp, "null").mkString("\t")
      }
    }.mkString("\n")

    val table = List(metaTab, bodyTab).mkString("\n")
    println(s"%table $table")
  }
}