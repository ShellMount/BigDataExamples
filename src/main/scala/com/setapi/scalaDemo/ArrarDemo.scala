package com.setapi.scalaDemo

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

object ArrarDemo {
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(1, 2, 3, 4, 5)

    println(s"数组大小: ${arr.size}")
    arr(4) = 100
    arr.foreach(println)

    // TODO: 数组的第二种定义，不常用
    val arr2 = new Array[Int](5)

    // TODO: 可以不接受不同的类型
    val arr3: Array[Any] = Array(1, 3, "Hello")

    // TODO: 转换数据类型: 父类转换为了类，得到更清晰的类型
    val intValue = arr3(0).asInstanceOf[Int]

    // TODO: 可变数组：允许添加或删除元素
    val arrBuffer = ArrayBuffer[Int]()

    // 添加元素
    arrBuffer += 3
    arrBuffer += 5
    arrBuffer += 7
    arrBuffer += 9

    println(s"arrBuffer= ${arrBuffer}")
    arrBuffer(0) = 113
    println(s"arrBuffer= ${arrBuffer}")

    // 添加一组元素
    arrBuffer ++= Array(11, 13, 15)
    println(s"arrBuffer= ${arrBuffer}")

    /**
      * 在实际 中，将可变数组，转换为不可变数组，然后进行计算与操作
      */
    val arr4 = arrBuffer.toArray
    arr4.foreach(println)

    // TODO: 常用的makeString
    println(arr4.mkString(", "))
    println(arr4.mkString("<", ", ", ">"))




  }
}
