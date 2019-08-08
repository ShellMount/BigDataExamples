package com.setapi.api.hdfs

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap
import scala.io.Source

/**
  *
  * Created by ShellMount on 2019/7/9
  *
  **/

case class IP(address: String, _type: String)
case class Access()

object BussinessTopology {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("业务拓扑图").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile("E:\\APP\\Tmp\\NetworkData\\201907072135")
    inputFile.map(line => {
      val arr = line.split(",")
      (arr(3), arr(4))
    }).flatMap(line => line._1)

    println(inputFile.count())
    println(inputFile.first())

    println(inputFile.flatMap(line => {line.split(",")}).countByValue().filter(parner => parner._2 > 1000).toSeq.sortWith(_._2 > _._2)
    )


    /////////////////////////
    val grades = Map( "Kim" -> 90 ,
      "Al" -> 85 ,
      "Melissa" -> 95 ,
      "Emily" -> 91 ,
      "Hannah" -> 92
    )

    println(grades)

    println(ListMap(grades.toSeq.sortBy(_._2):_*))

    /////
    val l = List("apple", "banana", "cherry")

    println(l)


  }
}
