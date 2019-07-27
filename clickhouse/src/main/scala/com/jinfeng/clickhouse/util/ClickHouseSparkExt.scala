package com.jinfeng.clickhouse.util

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019-07-19
  * @time: 17:23
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object ClickHouseSparkExt {
  implicit def extraOperations(df: org.apache.spark.sql.DataFrame) = DataFrameExt(df)
}
