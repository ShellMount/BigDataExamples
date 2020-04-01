package com.setapi.flink_user_behavior_analysis.login_fail

import java.sql.Timestamp

import com.setapi.flink_user_behavior_analysis.pv_uv_analysis.PageView.getClass
import com.setapi.flink_user_behavior_analysis.pv_uv_analysis.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 用户登陆行为统计
  */
object LoginFail2 {
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
    val warningStream = dataStream
      .keyBy(_.userId)
      .process(new LoginDetect(3))

    // 4. sink: 控制台输出
    warningStream.print("login fail count")
    env.execute("login fail job")
  }
}

/**
  * N秒内连续失败 maxFailTimes 后判断为失败
  *
  * KeyedProcessFunction：
  * 为每个key，来的每一条数据处理一次
  *
  * @param maxFailTimes
  */
class LoginDetect2(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning] {
  // 定义时间长度: 秒
  lazy val timeLength = 2
  // 保存2秒内所有的失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    // 判断类型是否为fail，只添加fail到状态
    if (value.eventType == "fail") {
      // 失败时，判断之前是否有失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        // 有失败事件，比较时间间隔
        val firstFail = iter.next()
        if (value.eventTime < firstFail.eventTime + timeLength) {
          // 两次间隔小于2秒，输出告警
          val warningMsg = s"连续 ${timeLength} 毫秒内登陆失败次数超过 ${maxFailTimes}"
          out.collect(LoginWarning(value.userId, firstFail.eventTime, value.eventTime, warningMsg))
        }
        // 更新最近一次的登陆失败事件
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 第一次失败
        loginFailState.add(value)
      }
    } else {
      // 成功时清空状态
      loginFailState.clear()
    }

  }
}

