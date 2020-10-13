package com.jinfeng.clickhouse.dsp

/**
  * @package: mobvista.dmp.clickhouse.dsp
  * @author: wangjf
  * @date: 2019-10-17
  * @time: 10:25
  * @email: jinfeng.wang@mobvista.com
  * @phone: 152-1062-7698
  */
object Constant {

  val indexColumn: Seq[String] = Seq("device_id", "platform","country", "install_apps")

  val orderColumn: Seq[String] = Seq("device_id", "platform")
}
