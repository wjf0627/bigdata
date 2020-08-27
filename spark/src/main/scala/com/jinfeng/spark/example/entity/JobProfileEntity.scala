package com.jinfeng.spark.example.entity

import java.util.Date

/**
  * @package: com.jinfeng.spark.example.entity
  * @author: wangjf
  * @date: 2019-06-10
  * @time: 17:02
  * @emial: wjf20110627@163.com
  * @phone: 152-1062-7698
  */
case class JobProfileEntity(job_id: String, job_name: String, user_id: String, country: String, platform: String, package_name: String, last_req_day: String, dimension: String,
                            behavior: String, flag: String, path: String, limit: String, create_time: String, return_data_num: Int) extends java.io.Serializable