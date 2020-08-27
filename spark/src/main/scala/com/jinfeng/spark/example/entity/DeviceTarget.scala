package com.jinfeng.spark.example.entity


/**
  * @package: com.jinfeng.spark.example.entity
  * @author: wangjf
  * @date: 2019/5/16
  * @time: 下午1:47
  * @email: wjf20110627@163.com
  * @phone: 152-1062-7698
  */

case class DeviceTarget(device_id: String, target_offer_list: Set[Integer]) extends Serializable
