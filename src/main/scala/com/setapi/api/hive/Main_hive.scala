package com.setapi.api.hive

/**
  *
  * Created by ShellMount on 2019/7/13
  *
  **/

object Main_hive {
  def main(args: Array[String]): Unit = {

    val hive = new HiveApi
    val word = null

    println(hive.lower(word))

  }
}
