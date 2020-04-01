package com.setapi.flink_user_behavior_analysis.login_fail

case class LoginEvent(
                     userId: Long,
                     ip: String,
                     eventType: String,
                     eventTime: Long
                     )
