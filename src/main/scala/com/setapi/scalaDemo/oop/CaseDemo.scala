package com.setapi.scalaDemo.oop

import java.io.FileNotFoundException

/**
  *
  * Created by ShellMount on 2019/8/12
  *
  **/

object CaseDemo {

  /**
    * 异常集中处理
    * @param e
    */
  def processException(e: Exception): Unit = {
    e match {
      case e1: IllegalArgumentException => println("Illegal Argument Exception")
      case e2: FileNotFoundException => println("File can not be found")
      case e3: ArithmeticException => println("Math Arithmetic Exception")
      case e4: NumberFormatException => println("Number Format Exception")
      case _e: Exception => _e.printStackTrace()
    }
  }

  /**
    * 成绩分类处理
    * @param grade
    * @param name
    */
  def judgeGrade(grade: String, name: String) = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case _ if "zhangshan".equals(name) => println("Come on...")
      case _grade => println(s"${name} grade is ${grade}, you need to work hard")


    }
  }

  /**
    * 对元组数据进行模式匹配操作
    * @param tuple
    */
  def match_tuple(tuple: Any): Unit = {
    tuple match {
      case (0, _) => println("tuple._1 = 0")
      case (_, 0) => println("tuple._2 = 0")
      case _ => println("no zero be found")
    }
  }


  def main(args: Array[String]): Unit = {

    judgeGrade("A", "")
    judgeGrade("B", "")
    judgeGrade("C", "")
    judgeGrade("E", "zhangshan")
    judgeGrade("E", "xxx")

    try {
      val result = 1 / 0
    } catch {
      case e: Exception => processException(e)
    }

    match_tuple((0, 1))

    val Array(name, age, gender) = "sz,30,male".split(",")
    println(s"${name} + ${age} + ${gender}")
  }
}
