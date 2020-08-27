package com.jinfeng.flink.example

import scala.collection.mutable

case class DeviceCase(device_id: String, age: String, gender: String, frenquency: mutable.WrappedArray[String])
