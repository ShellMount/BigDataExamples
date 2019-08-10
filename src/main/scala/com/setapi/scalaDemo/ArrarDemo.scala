package com.setapi.scalaDemo

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


  }
}
