package com.setapi.scalaDemo.collections

import scala.collection.mutable

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  * 数学意义上的集合，无序不重复元素
  **/

object SetDemo {
  def main(args: Array[String]): Unit = {

    var set: Set[Any] = Set(1, 2.0, "Spark")
    println(s"set=${set}")
    // 集合的添加，会生成一个新的集合
    set += 777

    println(s"添加后的set=${set}")
    println(s"添加后的set=${set + 1000}")
    println(s"添加后的set=${set}")

    // max, min ： 没有可比性
//    println(s"max=${set.maxBy(x => Math.abs(_))}")
//    println(s"min=${set.minBy(x => Math.abs(_))}")

    val set2 = mutable.Set[Int]()
    set2 += 7
    set2 ++= Set(1, 3, 5)
    set2 ++= List(7, 9, 9, 9, 9)
    set2.toSet.foreach(println)

  }
}
