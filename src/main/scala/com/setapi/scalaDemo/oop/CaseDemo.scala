package com.setapi.scalaDemo.oop

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

object CaseDemo {

  def judgeGrade(grade: String) = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case _ => println("You need to work harder")

    }
  }
  def main(args: Array[String]): Unit = {

    judgeGrade("A")
    judgeGrade("B")
    judgeGrade("C")
    judgeGrade("E")

  }
}
