package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

class SignPen {
  def write(name: String) = println(s"name = ${name}")

}

object SignPen {
  // 隐式作用域能读取到的地方即可
  implicit val signPen = new SignPen
}

/**
  * 参数的隐匿转换，是默认/自动的实例化另一个对象在本函数中使用
  */
object ImplicitParameterDemo {
  def signForMeetup(name: String)(implicit signPen: SignPen): Unit = {
    signPen.write(name)
  }

  def main(args: Array[String]): Unit = {

    // 到了考场签到
    signForMeetup("xieyie")

  }
}
