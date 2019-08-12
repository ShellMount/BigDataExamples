package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  * 避免出现NullPointerException异常
  * Option:
  *   Some(1)
  *   None
  **/

object OptionDemo {
  def main(args: Array[String]): Unit = {
    val map = Map(
      "AA" -> 1,
      "BB" -> 2
    )

    val aaValue: Option[Int] = map.get("AA")
    println(s"aaValue=${aaValue.get}")


    val mapValue = map.get("XX") match {
      case Some(value) => value
      case None => 0
    }

    println(s"mapValue=${mapValue}")

    if(!map.get("XX").isEmpty) println(s"XX = ${map.get("XX")}")

    val xx = map.getOrElse("XX", 0)
    println(s"XX = ${xx}")

  }
}
