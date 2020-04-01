package com.setapi.flink_user_behavior_analysis.hot_items_analysis

/**
  * 输入数据
  * @param userId
  * @param itemId
  * @param categoryId
  * @param behavior
  * @param timestamp
  */
case class UserBehavior(
                       userId: Long,
                       itemId: Long,
                       categoryId: Int,
                       behavior: String,
                       timestamp: Long
                       )
