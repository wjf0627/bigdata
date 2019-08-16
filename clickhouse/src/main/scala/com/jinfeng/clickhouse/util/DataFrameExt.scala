package com.jinfeng.clickhouse.util

import com.jinfeng.clickhouse.util.ClickHouseResultSetExt._
import com.jinfeng.clickhouse.util.Utils._
import org.apache.spark.sql.types._
import ru.yandex.clickhouse.ClickHouseDataSource

import scala.collection.mutable

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019-07-16
  * @time: 18:28
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */

case class DataFrameExt(df: org.apache.spark.sql.DataFrame) extends Serializable {

  def dropClickHouseDb(dbName: String, clusterNameO: Option[String] = None)
                      (implicit ds: ClickHouseDataSource) {
    val client = ClickHouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.dropDb(dbName)
      case Some(x) => client.dropDbCluster(dbName)
    }
  }

  def createClickHouseDb(dbName: String, clusterNameO: Option[String] = None)
                        (implicit ds: ClickHouseDataSource) {
    val client = ClickHouseClient(clusterNameO)(ds)
    clusterNameO match {
      case None => client.createDb(dbName)
      case Some(x) => client.createDbCluster(dbName)
    }
  }

  def createClickHouseTable(dbName: String, tableName: String, partitionColumnNames: Seq[String], indexColumns: Seq[String], orderColumnNames: Seq[String], clusterNameO: Option[String] = None)
                           (implicit ds: ClickHouseDataSource) {
    val client = ClickHouseClient(clusterNameO)(ds)
    val sqlStmt = createClickHouseTableDefinitionSQL(dbName, tableName, partitionColumnNames, indexColumns, orderColumnNames)
    clusterNameO match {
      case None => client.query(sqlStmt)
      case Some(clusterName) =>
        // create local table on every node
        client.queryCluster(sqlStmt)
        // create distrib table (view) on every node
        val sqlStmt2 = s"CREATE TABLE IF NOT EXISTS ${dbName}.${tableName}_all AS ${dbName}.${tableName} ENGINE = Distributed($clusterName, $dbName, $tableName, rand());"
        client.queryCluster(sqlStmt2)
    }
  }

  def saveToClickHouse(dbName: String, tableName: String, partitionVals: Seq[String], partitionColumnNames: Seq[String], clusterNameO: Option[String] = None, batchSize: Int = 100000)
                      (implicit ds: ClickHouseDataSource) = {

    val defaultHost = ds.getHost
    val defaultPort = ds.getPort

    val (clusterTableName, clickHouseHosts) = clusterNameO match {
      case Some(clusterName) =>
        // get nodes from cluster
        val client = ClickHouseClient(clusterNameO)(ds)
        (s"${tableName}_all", client.getClusterNodes())
      case None =>
        (tableName, Seq(defaultHost))
    }

    val schema = df.schema

    // following code is going to be run on executors
    val insertResults = df.rdd.mapPartitions((partition: Iterator[org.apache.spark.sql.Row]) => {

      val rnd = scala.util.Random.nextInt(clickHouseHosts.length)
      val targetHost = clickHouseHosts(rnd)
      val targetHostDs = ClickHouseConnectionFactory.get(targetHost, defaultPort)

      // explicit closing
      using(targetHostDs.getConnection) { conn =>

        val descSql = s"desc $dbName.$tableName"
        val descStatement = conn.prepareStatement(descSql)

        val results = descStatement.executeQuery()
        val columns = results.map(x => x.getString("name")).reverse

        val insertStatementSql = generateInsertStatment(schema, dbName, clusterTableName, columns)
        val statement = conn.prepareStatement(insertStatementSql)

        var totalInsert = 0
        var counter = 0

        while (partition.hasNext) {

          counter += 1
          val row = partition.next()

          val offSet = partitionColumnNames.size + 1
          // create mock date
          for (i <- partitionVals.indices) {
            if (i == 0) {
              statement.setDate(i + 1, java.sql.Date.valueOf(partitionVals(i)))
            } else {
              statement.setString(i + 1, partitionVals(i))
            }
          }

          // map fields
          schema.foreach { f =>
            val fieldName = f.name
            val fieldIdx = row.fieldIndex(fieldName)
            val fieldVal = row.get(fieldIdx)
            if (fieldVal != null) {
              val obj = if (fieldVal.isInstanceOf[mutable.WrappedArray[_]]) {
                fieldVal.asInstanceOf[mutable.WrappedArray[_]].array
              } else {
                fieldVal
              }
              statement.setObject(fieldIdx + offSet, obj)
            } else {
              val defVal = defaultNullValue(f.dataType, fieldVal)
              statement.setObject(fieldIdx + offSet, defVal)
            }
          }
          statement.addBatch()

          if (counter >= batchSize) {
            val r = statement.executeBatch()
            totalInsert += r.sum
            counter = 0
          }

        } // end: while

        if (counter > 0) {
          val r = statement.executeBatch()
          totalInsert += r.sum
          counter = 0
        }

        // return: Seq((host, insertCount))
        List((targetHost, totalInsert)).toIterator
      }

    }).collect()

    // aggr insert results by hosts
    insertResults.groupBy(_._1)
      .map(x => (x._1, x._2.map(_._2).sum))
  }

  private def generateInsertStatment(schema: org.apache.spark.sql.types.StructType, dbName: String, tableName: String, columns: Seq[String]) = {
    /*
    var columns: scala.List[String] = scala.List()
    for (i <- partitionColumnNames.indices) {
      columns = columns.::(s"${partitionColumnNames(i)}")
    }
    columns = columns.reverse
    columns = schema.map(f => f.name).toList.:::(columns)
     */
    //  val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to columns.length map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def defaultNullValue(sparkType: org.apache.spark.sql.types.DataType, v: Any) = sparkType match {
    case DoubleType => 0
    case LongType => 0
    case FloatType => 0
    case IntegerType => 0
    case StringType => null
    case BooleanType => false
    case _ => null
  }

  private def createClickHouseTableDefinitionSQL(dbName: String, tableName: String, partitionColumnNames: Seq[String], indexColumns: Seq[String], orderColumnNames: Seq[String]) = {

    val header =
      s"""
          CREATE TABLE IF NOT EXISTS $dbName.$tableName(
          """
    var columns: scala.List[String] = scala.List()
    var dt = ""
    var parts: scala.List[String] = scala.List()
    for (i <- partitionColumnNames.indices) {
      if (i == 0) {
        dt = partitionColumnNames(i)
        columns = columns.::(s"${partitionColumnNames(i)} Date")
      } else {
        parts = parts.::(partitionColumnNames(i))
        val timeset = Set("hour", "minute", "second", "hh", "mm", "ss")
        val partType = if (timeset.contains(partitionColumnNames(i).toLowerCase)) {
          "FixedString(2)"
        } else {
          "String"
        }
        columns = columns.::(s"${partitionColumnNames(i)} ${partType}")
      }
    }
    columns = columns.reverse
    columns = df.schema.map { f =>
      Seq(f.name, sparkType2ClickHouseType(f.dataType)).mkString(" ")
    }.toList.:::(columns)

    /*
    val columns = s"$partitionColumnName Date" :: df.schema.map { f =>
      Seq(f.name, sparkType2ClickHouseType(f.dataType)).mkString(" ")
    }.toList
     */
    val columnsStr = columns.mkString(",\n")

    val footer =
      if (parts.isEmpty) {
        if (orderColumnNames.isEmpty) {
          s"""
             |)ENGINE = MergeTree(${partitionColumnNames.mkString(",")}, (${indexColumns.mkString(",")}), 8192);
           """.stripMargin
        } else {
          s"""
             |)ENGINE = MergeTree(${partitionColumnNames.mkString(",")}, (${orderColumnNames.mkString(",")}), 8192);
           """.stripMargin
        }
      } else {
        s"""
           |)ENGINE = MergeTree()
         """.stripMargin
      }

    val partitioner =
      if (parts.nonEmpty) {
        if (orderColumnNames.isEmpty) {
          s"""
             |PARTITION BY (toYYYYMMDD(${dt}),${parts.mkString(",")}) ORDER BY (${dt},${parts.mkString(",")},${indexColumns.mkString(",")})
           """.stripMargin
        } else {
          s"""
             |PARTITION BY (toYYYYMMDD(${dt}),${parts.mkString(",")}) ORDER BY (${dt},${parts.mkString(",")},${orderColumnNames.mkString(",")})
           """.stripMargin
        }
      } else {
        ""
      }
    Seq(header, columnsStr, footer, partitioner).mkString("\n")
  }

  private def sparkType2ClickHouseType(sparkType: org.apache.spark.sql.types.DataType) = sparkType match {
    case LongType => "Int64"
    case DoubleType => "Float64"
    case FloatType => "Float32"
    case IntegerType => "Int32"
    case StringType => "String"
    case BooleanType => "UInt8"
    case ArrayType(IntegerType, true) => "Array(Int32)"
    case ArrayType(StringType, true) => "Array(String)"
    case _ => "unknown"
  }

}

