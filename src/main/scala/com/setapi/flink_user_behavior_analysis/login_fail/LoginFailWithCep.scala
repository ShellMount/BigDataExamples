package com.setapi.flink_user_behavior_analysis.login_fail

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 用户登陆行为统计
  * 使用CEP作模式匹配
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    val resource = getClass.getResource("/").toString.replace("target/classes/", "") + "data/flink/LoginLog.csv"
    val dataStream = env.readTextFile(resource)
      .map(record => {
        val dataArray = record.split(",")
        LoginEvent(
          dataArray(0).trim.toLong,
          dataArray(1).trim,
          dataArray(2).trim,
          dataArray(3).trim.toLong
        )
      })
      // 乱序数据: 指定时间戳与水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 3. transform 处理数据
    val groupStream = dataStream
      .keyBy(_.userId)
      // .process(new LoginDetect(3))

    // 4. 定义匹配模式：对乱序数据友好
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
        .next("next").where(_.eventType == "fail")
        .within(Time.seconds(2))

    // 5. 在事件流上应用模式
    val patternStream = CEP.pattern(groupStream, loginFailPattern)

    // 6. 从patter stream 上应用 select function 抽出匹配的事件序列
    val loginFailStream = patternStream.select(new LoginFailMatch())

    // 7. sink: 控制台输出
    loginFailStream.print("login fail with cep")
    env.execute("login fail with cep job")
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, LoginWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {
    // 从MAP中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    LoginWarning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}

