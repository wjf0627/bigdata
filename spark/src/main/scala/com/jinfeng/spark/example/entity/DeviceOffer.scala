package com.jinfeng.spark.example.entity

import scala.collection.mutable

/**
  * @package: com.jinfeng.spark.example.entity
  * @author: wangjf
  * @date: 2019/5/16
  * @time: 下午1:47
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */

case class DeviceOffer(device_id: String, age: String, behavior: mutable.WrappedArray[String], gender: String, install_apps: mutable.WrappedArray[String], interest: String, target_offer_list: mutable.WrappedArray[String]) extends Serializable
