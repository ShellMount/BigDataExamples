package com.setapi.flink_user_behavior_analysis.pv_uv_analysis

/**
  * 定义一个布隆过滤器
  * @param size 过滤器支持大小
  */
class Bloom(size: Long) extends Serializable {

  // 位图的总大小: 默认16兆字节： 2^^4 * 2^^10 * 2^^10 * 2^^3(转字节)
  private val cap = if (size > 0) size else 1 << 27

  // 定义Hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }

    result & (cap - 1)
  }
}
