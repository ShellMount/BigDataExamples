package com.setapi.sparkDemo.logDemo

/**
  *
  * Created by ShellMount on 2019/8/25
  *
  * 自定义排序的工具类
  **/

object OrderingUtils {

  object SecondValueOrdering extends Ordering[(String, Int)] {
    /**
      * 比较第二值的大小
      * @param x
      * @param y
      * @return
      */
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      x._2.compare(y._2)
    }
  }
}
