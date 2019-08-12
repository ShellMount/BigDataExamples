package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

object CaseDemo {

  def judgeGrade(grade: String, name: String) = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case _ if "zhangshan".equals(name) => println("Come on...")
      case _grade => println(s"${name} grade is ${grade}, you need to work hard")


    }
  }
  def main(args: Array[String]): Unit = {

    judgeGrade("A", "")
    judgeGrade("B", "")
    judgeGrade("C", "")
    judgeGrade("E", "zhangshan")
    judgeGrade("E", "xxx")

  }
}
