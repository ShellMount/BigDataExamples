package com.setapi.flink_user_behavior_analysis.login_fail

case class LoginWarning(
                       userId: Long,
                       firstFailTime: Long,
                       lastFailTime: Long,
                       wariningMsg: String
                       )
