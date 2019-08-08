package com.setapi.api.hdfs


/**
  *
  * Created by ShellMount on 2019/7/2
  *
  **/

object Main_hdfs {
  def main(args: Array[String]): Unit = {
    println("I am gonna into BigData")
    val dfs = new HdfsApi()
    dfs.getHadoopFileSystem
    // https://www.cnblogs.com/yanghuabin/p/7628088.html
    dfs.createDirectory()
    //dfs.rmDirectory()
    dfs.putFile2HDFS()
    dfs.getFileFromHDFS
    dfs.copyBetweenHDFS
  }


}
