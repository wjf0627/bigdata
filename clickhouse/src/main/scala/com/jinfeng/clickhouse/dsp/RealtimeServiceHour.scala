package com.jinfeng.clickhouse.dsp

import scala.collection.mutable

/**
  * @package: mobvista.dmp.clickhouse.dsp
  * @author: wangjf
  * @date: 2019-10-18
  * @time: 14:20
  * @email: jinfeng.wang@mobvista.com
  * @phone: 152-1062-7698
  */
case class RealtimeServiceHour(device_id: String, platform: String, age: Int, gender: Int, country: String,
                               interest: mutable.WrappedArray[String], install_apps_keys: mutable.WrappedArray[Int],
                               install_apps_dates: mutable.WrappedArray[String], frequency: String, version: Long) extends java.io.Serializable