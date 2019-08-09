package com.setapi.scalaDemo

/**
  *
  * Created by ShellMount on 2019/8/9
  *
  **/

object HowMuchParameters {

  def howMuchParameters(courses: String*): Unit = {
    println(s"接收一系列的参数: ${courses}")

    courses.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    howMuchParameters("ni hao ma", "bu hao")

    val arr = Array("HADOOP", "SPARK", "HIVE", "HBASE")
    howMuchParameters(arr:_*)


  }
}
