package com.setapi.scalaDemo.collections

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

object TupleDemo {
  def main(args: Array[String]): Unit = {
    // 数组
    val array: Array[Any] = Array(1, 2.0, "spark")
    println("数组: " + array(0).asInstanceOf[Int])

    // 元组
    val tuple3 = (1, 2.0, "spark")
    println("元组: " + tuple3._1)

    // 二元组
    val tuple2: (Double, String) = (2.0, "spark")
    println("元组: " + tuple2._1)

    // 二元组互换
    println("二元组互换: " + tuple2.swap)

    // 获取元组中的元素：嵌套的元组
    val t1 = ("key", ("key2", "value"))

    // 创建一个另类的二元组
    val t2 = "name" -> "xieyi"



  }
}
