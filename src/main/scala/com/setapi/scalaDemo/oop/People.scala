package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/11
  *
  **/

class People(val sex: String) {

  println("---------正在创建对象---------")
  // 属性: 通常是名词
  // var 定义的变量，表明值是可变的，scala编译时自动生成 getter/setter
  var name: String = _

  // val 定义的变量，表明值不可变，scala编译时只生成 getter
  private val age = 10

  /**
    * 构造方法
    * 附属构造方法，第一行，必须调用已经存在的主构造方法，或其它附属构造方法
    */
  def this(_name: String, _age: Int, _sex: String) {
    this(_sex)
  }

  /**
    * 定义方法
    */
  def eat(): String ={
    name + " is eating.."
  }
}


object People {
  def apply(): People = new People("male")

  def apply(name: String, sex: String): People = {
    val p = new People("male")
    p.name = name
    p
  }


  def main(args: Array[String]): Unit = {
    // 伴生对象中创建的对象
    val people = People("Xie Yi", "male")
    println(s"people.name = ${people.age},  people.age = ${people.age}")

    // 附属构造中创建的对象
    val people2 = new People("xieyi", 20, "male")


  }
}
