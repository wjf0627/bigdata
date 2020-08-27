package com.jinfeng.spark.example.entity

import scala.collection.mutable

/**
  * @package: com.jinfeng.spark.example.entity
  * @author: wangjf
  * @date: 2019-06-11
  * @time: 16:31
  * @emial: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
case class DeviceTag(device_id: String, age: String, gender: String, install_apps: mutable.WrappedArray[String], interest: mutable.WrappedArray[String],
                     frequency: mutable.WrappedArray[structs]) extends java.io.Serializable

case class structs(tag: String, cnt: String)
