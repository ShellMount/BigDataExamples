package com.setapi.scalaDemo

import scala.util.control.Breaks

/**
  *
  * Created by ShellMount on 2019/8/9
  *
  **/

object BreakDemo {
  def main(args: Array[String]): Unit = {
    val numList = List(1, 2, 3, 4, 5, 6)

    // 创建一个Breaks实例
    val loop = new Breaks()

    // 对要遍历的循环进行控制
    loop.breakable(
      for (num <- numList if num == 5) {
        println(s"value of is : ${num}")

        // 跳出循环
        loop.break()
      }
    )

  }
}
