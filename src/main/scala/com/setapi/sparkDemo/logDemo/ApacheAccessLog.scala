package com.setapi.sparkDemo.logDemo

import scala.util.matching.Regex

/**
  *
  * Created by ShellMount on 2019/8/25
  *
  **/

case class ApacheAccessLog (
ipAddress: String,
clientIdented: String,
userId: String,
dateTime: String,
method: String,
endpoint: String,
protocol: String,
responseCode: Int,
contentSize: Long)

object ApacheAccessLog {
  // 正则表达式, 以^开头，以$结束
  // 1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /chapter1/java/src/main/java/com/databricks/apps/logs/LogAnalyzer.java HTTP/1.1" 200 1234
  val PARTTERN: Regex ="""^(\S+) (-|\S+) (-|\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)$""".r

  /**
    * 用于解析LOG文件，将每行数据解析成对应的CASE CLASS
    * @param log
    * @return
    */
  def parseLogLine(log: String): ApacheAccessLog = {
    // 使用正则表达式进行匹配
    val res: Option[Regex.Match] = PARTTERN.findFirstMatchIn(log)

    if(res.isEmpty) {
      throw new RuntimeException(s"Cannot parse log line: ${log}")
    }

    // 获取值
    val m: Regex.Match = res.get

    ApacheAccessLog(
      m.group(1),
      m.group(2),
      m.group(3),
      m.group(4),
      m.group(5),
      m.group(6),
      m.group(7),
      m.group(8).toInt,
      m.group(9).toLong
    )
  }

  /**
    * 用于对数据过滤，不符合正则的数据过滤掉，否则后续解析出错
    * @param log
    * @return
    */
  def isValidateLogLine(log: String): Boolean = {
    // 使用正则表达式进行匹配
    val res: Option[Regex.Match] = PARTTERN.findFirstMatchIn(log)

    // 返回不为空时的数据则保留
    !res.isEmpty
  }
}