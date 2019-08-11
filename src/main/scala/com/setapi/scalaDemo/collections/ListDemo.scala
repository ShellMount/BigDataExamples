package com.setapi.scalaDemo.collections

import scala.collection.mutable.ListBuffer

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

object ListDemo {

  def main(args: Array[String]): Unit = {
    // 创建List
    val list = List(1, 2, 3, 4, 4, 4, 5, 6, "SPARK")
    println(s"list size = ${list.size}, list length = ${list.length}")

    // TODO: LIST组成
    // TODO: list由 head + tail 组成，除了head以外的都是tail
    println(s"list head = ${list.head}")
    println(s"list tail = ${list.tail}")

    // TODO: 创建List的第二种方式, 空集合可以使用Nil表示
    val list3 = 1 :: Nil
    println(s"list3 head = ${list3.head}")
    println(s"list3 tail = ${list3.tail}")

    // TODO: 将list元素转换为字符 串
    println(s"list3 的字符串: ${list3.mkString(",")}")

    // TODO: 采用Nil进行元素的构建: 这是从右向左的构建
    val list4 = 3 :: 2:: 1 :: Nil
    println(s"list4 = ${list4}")
    val list5 = List(1, 3, 5, 7) ::: List(2, 4, 6)
    println(s"list5 = ${list5}")


    // TODO: 元素下标
    println(s"list5 的第三个元素 = ${list5(2)}")

    /**
      * 可变的集合
      */
    val listBuffer = ListBuffer[Int]()
    listBuffer += 1
    listBuffer += 11
    listBuffer += 111
    listBuffer += (2, 22, 222)
    listBuffer ++= list4
    println(s"listBuffer=${listBuffer}")

    // 对可变集合的操作
    println(s"listBufferString: ${listBuffer.mkString(", ")}")
    val list6 = listBuffer.toList
    println(s"list6: ${list6.mkString(", ")}")
    println(s"list6 is empty: ${list6.isEmpty}")

    /**
      * 常用方略
      */

    /**
      * 集合的操作: 高阶函数
      */
    // TODO: map / flatMap / withFilter / filter
    val listMap = list6.map(x => { x * 2.0 })
    println(s"map func: ${listMap}")

    // TODO: flatMap: 对list中每个元素进行给定方法的调用，要求给定参数的结果是一个集合
    // 类似一对多的概念：一个输入返回多个输出
    val listFlatMap = list6.flatMap(x => {
      val arr = (0 to x).toList
      arr
    })
    println("listFlatMap:")
    listFlatMap.foreach(x => print(x + ", "))
    println()

    // TODO: filter, 元素过滤
    listFlatMap.filter(x => x % 2 == 0 ).foreach(print)
    println()
    listFlatMap.filterNot(x => x % 2 == 0 ).foreach(print)
    println("\n------------------")


    // TODO: 元素进行分组
    println("分组的List:")
    listFlatMap.groupBy(x => x % 2).foreach(println)

    // TODO: 排序
    println("排序的List:")
    listFlatMap.sorted.foreach(print)
    println()

    println("指定排序的规则:")
    listFlatMap.sortBy(x => -x).foreach(print)
    println()

    println("指定排序的规则:")
    // 两个元素间的比较
    listFlatMap.sortWith((x, y) => x > y).foreach(print)
    println()

    /**
      * 也很重要的操作
      */
    // TODO: reduce
    // reduce输入与输出的类型要相同
    println(s"list5 = ${list5}")
    println("list5 reduce 求和: " + list5.reduce((a, b) => a + b))
    println("list5 reduce 求和: " + list5.sum)
    println("list5 reduce 求和: " + list5.reduceLeft(_ + _))
    println("list5 reduce 求和: " + list5.reduceRight(_ + _))

    // TODO: fold
    // TODO: fold(z: A1)(op: (A1, A1) => A1)
    // 第一个参数：初始值
    // 第二个参数：计算函数，类型于reduce

    println("list5 fold 求和: " + list5.fold(2)(_ + _))
    println("list5 fold 求和: " + list5.foldLeft(2)(_ + _))
    println("list5 fold 求和: " + list5.foldRight(2)(_ + _))
    println("list5 fold 求差: " + list5.fold(2)(_ - _))
    println("list5 fold 求差: " + list5.foldLeft(2)(_ - _))
    println("list5 fold 求差: " + list5.foldRight(2)(_ - _))

    /**
      * 集合的拉链操作
      * 转换集合中的元素为 K-V 对
      */
    println("集合的拉链操作: ")
    val zipList = list5.zipWithIndex
    zipList.foreach(println)

    // TODO: 合并
    val list7 = List(1, 2, 3, 4, 5)
    val list8 = List("a", "b", "c", "d", "e")
    val zipList2 = list7.zip(list8)
    println(zipList2)

    // TODO
    println(s"exists: ${list7.exists(_ % 2 == 0)}")
    println(s"forall: ${list7.forall(_ % 2 == 0)}")

  }
}
