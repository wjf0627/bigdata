package com.jinfeng.spark.example.example

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

/**
 * @package: com.jinfeng.spark.example.example
 * @author: wangjf
 * @date: 2020/9/16
 * @time: 5:08 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
object SparkUtil {

  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**
   * 打印所有的文件名，包括子目录
   */
  def listAllFiles(path: String) {
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile())
        println(path)
      else {
        listAllFiles(path.toString())

      }
    })
  }

  /**
   * 打印一级文件名
   */
  def listFiles(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile()).foreach(println)
  }

  /**
   * 打印一级目录名
   */
  def listDirs(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory()).foreach(println)
  }

  /**
   * 打印一级文件名和目录名
   */
  def listFilesAndDirs(path: String) {
    getFilesAndDirs(path).foreach(println)
  }
}
