package com.setapi.project.analystics

/**
  *
  * Created by ShellMount on 2019/8/6
  *
  **/

/**
  *
  * * 把RDD转换为DataFrame:
  * * 自定义SCHEMA
  * * 或反射Case class
  *
  * @param uuid
  * @param day
  * 封闭日期（当月的第几天）
  * @param platformDimension
  * @param browserDimension
  */
case class DayPlatfromBrowser(uuid: String, day: Int, platformDimension: String, browserDimension: String)
