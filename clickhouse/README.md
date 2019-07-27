
### Usage

[CKExample](https://github.com/wjf0627/bigdata/blob/master/clickhouse/src/main/scala/com/jinfeng/clickhouse/CKExample.scala)

### 核心代码
[DataFrameExt](https://github.com/wjf0627/bigdata/edit/master/clickhouse/src/main/scala/com/jinfeng/clickhouse/util/DataFrameExt.scala)

### 创建表
```scala
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
```

### saveToClickHouse
```scala
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

        val insertStatementSql = generateInsertStatment(schema, dbName, clusterTableName, partitionColumnNames)
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
```