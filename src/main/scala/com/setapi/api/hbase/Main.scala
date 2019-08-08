package com.setapi.api.hbase

/**
  *
  * Created by ShellMount on 2019/7/29
  *
  **/

object Main {
  def main(args: Array[String]): Unit = {
    val hb = new MyHbase()
    hb.getUserInfo("firstOnHbase", "10010")
    hb.putUserInfo("firstOnHbase", "10012")
    //hb.putUserInfoByBatch("firstOnHbase")
    hb.updateUserInfo("firstOnHbase", "10012")
    hb.scanTable("firstOnHbase", "10012", "12300")
    hb.createTable("NewTable")
    hb.createTable2("NewTable2")
    hb.dropTable("NewTable2")
  }
}
