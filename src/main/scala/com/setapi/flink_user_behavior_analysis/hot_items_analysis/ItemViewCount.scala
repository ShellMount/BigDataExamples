package com.setapi.flink_user_behavior_analysis.hot_items_analysis

/**
  * 统计聚合结果
  * @param itemId
  * @param windowEnd
  * @param count
  */
case class ItemViewCount(
                        itemId: Long,
                        windowEnd: Long,
                        count: Long
                        )
