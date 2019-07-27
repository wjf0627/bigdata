package com.jinfeng.clickhouse.util

/**
  * @package: com.jinfeng.clickhouse.util
  * @author: wangjf
  * @date: 2019/5/20
  * @time: 下午3:46
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
object Utils {
  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try {
      f(closeable)
    }
    finally {
      closeable.close()
    }
}