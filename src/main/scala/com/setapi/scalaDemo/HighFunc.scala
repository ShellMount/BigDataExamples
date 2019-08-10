package com.setapi.scalaDemo

/**
  *
  * Created by ShellMount on 2019/8/9
  *
  **/

object HighFunc {

  def greeting(name: String, title: String, func: (String, String) => Unit): Unit = {
    func(name, title)
  }

  def say(name: String, title: String) = println(s"早呀。 ${title}-${name}-${title}")

  def main(args: Array[String]): Unit = {
    greeting("谢毅", "经理", say)
    greeting("谢毅", "总监", say _)
    greeting("谢毅", "总监", say(_, _))
  }
}
