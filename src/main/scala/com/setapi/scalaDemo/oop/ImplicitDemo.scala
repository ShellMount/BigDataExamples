package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

/**
  * 普通人
  *
  * @param name
  */
class Man(val name: String)

/**
  * 将Man类型转换为SuperMan
  */
object Man {
  // 隐式作用域能读取到的地方即可
  implicit def man2Superman(man: Man): SuperMan = {
    new SuperMan(man.name)
  }
}

/**
  * 超人，有特殊功能：发激光
  *
  * @param name
  */
class SuperMan(val name: String) {
  def emitLaser = println("emit a laser ......")
}


/**
  * 类型的隐匿转换，是让一个实例类，具备另一个类的功能
  * 可以访问其类中的方法
  */
object ImplicitDemo {
  def main(args: Array[String]): Unit = {
    val man = new Man("super")

    // 由于超人是隐藏的，特殊时期变身（由普通人变为超人
    man.emitLaser
  }

}
