package com.setapi.etlstreaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  *
  * Created by ShellMount on 2019/8/1
  *
  * 单例模式
  * Lazy方式创建 ObjectMapper 实例对象
  *
  *
  **/

object EtlObjectMapperSingle {

  // 不被序列化
  @transient private var instance: ObjectMapper = _

  def getInstance(): ObjectMapper = {
    if(instance == null) {
      instance = new ObjectMapper()
      instance.registerModule(DefaultScalaModule)
    }
    instance
  }
}
