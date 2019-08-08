package com.setapi.etlstreaming

import java.util.Random


/**
  *
  * Created by ShellMount on 2019/7/31
  *
  **/

object EtlRandomUtil {
  def getRandomNum(bound: Int): Int = {
    val random = new Random()
    random.nextInt(bound)
  }
}
