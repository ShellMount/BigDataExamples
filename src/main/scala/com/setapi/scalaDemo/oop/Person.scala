package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

class Person(val name: String) extends HelloTrait with MakeFriendTrait {
  /**
    * 打招呼
    * @param name
    */
  override def sayHello(name: String): Unit = println(s"Hello, ${name}")

  /**
    * 交朋友
    * @param person
    */
  override def makeFriend(person: Person): Unit = println(s"My name is ${name}. are your ${person.name}?")

}
