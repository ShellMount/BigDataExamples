package com.setapi.flink_user_behavior_analysis.network_flow_analysis

case class ApacheLogEvent(
                         ip: String,
                         userId: String,
                         eventTime: Long,
                         method: String,
                         url: String
                         )
