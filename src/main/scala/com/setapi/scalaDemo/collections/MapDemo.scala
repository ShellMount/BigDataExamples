package com.setapi.scalaDemo.collections

import scala.collection.mutable

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

object MapDemo {
  def main(args: Array[String]): Unit = {
    // 不可变的MAP
    val map = Map(
      "key1" -> "Value1",
      "key2" -> "Value2"
    )
    println(s"map = ${map}")

    // 另一种定义
    val map2 = Map(("key1" -> "Value1"), ("key2" -> "Value2"))
    val map22 = Map(("key1", "Value1"), ("key2", "Value2"))
    println(s"map2 = ${map2}")

    // 获取k, v
    println(s"key1 = ${map2("key1")}")
    println(s"key1 = ${map2.get("key1").get}")
    println(s"key1 = ${map2.getOrElse("key1", "none")}")

    // 遍历
    for((k, v) <- map2) {
      println(s"key = ${k}, value = ${v}")
    }

    map2.foreach(println)
    // 传入的参数是tuple, 不是(k, v)
    map2.foreach(tuple2 => s"key = ${tuple2._1}, value = ${tuple2._2}")

    /**
      * 可变的MAP
      */
    val map3 = mutable.Map[String, String]()
    map3 += "A" -> "HADOOP"
    val t = ("B", "SPARK")
    map3 += t
    map3 += ("C" -> "HIVE")
    map3 ++= map2

    // TODO: MAP操作及转换
    val map4 = map3.toMap
    map4.foreach(println)
  }
}
