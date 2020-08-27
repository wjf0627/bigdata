package com.jinfeng.spark.example.entity

import scala.collection.mutable

case class struct(tag: String, cnt: Int)

//age: String, gender: String
case class DeviceInfoCase(device_id: String, frequency: mutable.WrappedArray[(String,String)]) extends Serializable
