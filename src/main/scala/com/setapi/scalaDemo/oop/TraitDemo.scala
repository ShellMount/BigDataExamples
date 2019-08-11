package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

object TraitDemo {
  def main(args: Array[String]): Unit = {
    val myself = new Person("ShellMount")
    val wangGe = new Person("Wang Er Xiao")

    myself.sayHello(wangGe.name)
    myself.makeFriend(wangGe)
  }
}
