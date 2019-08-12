package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  * 样例类
  *
  **/

class Person2

case class Teacher(name: String, subject: String) extends Person2

case class Student(name: String, classroom: String) extends Person2

object CaseClassDemo {
  def judgeIndentify(p: Person2): Unit = {
    p match {
      case Teacher(name, subject) => println(s"name=${name}, subject=${subject}")
      case Student(name, classroom) => println(s"name=${name}, classroom=${classroom}")
    }
  }

  def main(args: Array[String]): Unit = {

    /**
      * 样例类的模式匹配
      */
    judgeIndentify(new Teacher("teacher-Wang", "English"))
    judgeIndentify((new Student("ShellMount", "302")))
  }
}
