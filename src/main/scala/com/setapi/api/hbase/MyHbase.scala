package com.setapi.api.hbase

import java.util.ArrayList

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Get, HBaseAdmin, Put, ResultScanner, Scan, Table, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.IOUtils


/**
  *
  * Created by ShellMount on 2019/7/29
  *
  **/

class MyHbase {
  /**
    * if hbase-site.xml is not definded or in a dev env, they are necessary:
    *   conf.set("hbase.zookeeper.property.clientPort", "2181")
    *   conf.set("hbase.zookeeper.quorum", "hnamenode")
    **/
  val conf = HBaseConfiguration.create()
  val conn = ConnectionFactory.createConnection(conf)

  /**
    * rowKey 10001  column  value
    *
    * @param rowKey
    */
  def getUserInfo(tableName: String, rowKey: String): Unit = {
    val table = getHTable(tableName)
    try {
      val get = new Get(toBytes(rowKey))
      get.addColumn(toBytes("info"), toBytes("name"))
      get.addColumn(toBytes("info"), toBytes("age"))

      val rs = table.get(get)
      val cells = rs.rawCells()
      for (cell: Cell <- cells) {
        // println("-----------" + Bytes.toString(CellUtil.cloneRow(cell)) + ":")
        print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":")
        println(Bytes.toString(CellUtil.cloneQualifier(cell)) + "--->" + Bytes.toString(CellUtil.cloneValue(cell)))
      }
    } catch {
      case e: TableNotFoundException => {
        println("不存在的表: " + tableName + "--->" + e.printStackTrace())
      }
      case t: Exception => {
        println(t.printStackTrace())
      }
    } finally {
      closeHTable(table)
    }
  }

  def putUserInfo(tableName: String, rowKey: String): Unit = {
    val table = getHTable(tableName)
    try {
      val put = new Put(toBytes(rowKey))
      put.addColumn(toBytes("info"), toBytes("name"), toBytes("ZhangShan"))
      put.addColumn(toBytes("info"), toBytes("age"), toBytes("19"))
      put.addColumn(toBytes("info"), toBytes("sex"), toBytes("男"))
      table.put(put)
    } catch {
      case e: TableNotFoundException => {
        println("不存在的表: " + tableName + "--->" + e.printStackTrace())
      }
      case t: Exception => {
        println(t.printStackTrace())
      }
    } finally {
      closeHTable(table)
    }
  }

  /**
    * @param tableName
    * @return
    */
  def getHTable(tableName: String): Table = {
    // can not access HTable, why?
    // new HTable(conf, tbName)
    val table = conn.getTable(TableName.valueOf(tableName))
    table
  }

  def closeHTable(htbl: Table): Unit = {
    if (htbl != null) {
      htbl.close()
    }
  }

  def putUserInfoByBatch(tableName: String): Unit = {
    val table = getHTable(tableName)
    try {
      // 这里不能使用 Scala-List?
      var l = new ArrayList[Put]
      for (i <- 10013 to 20000) {
        val put = new Put(Bytes.toBytes(i.toString))
        put.addColumn(toBytes("info"), toBytes("name"), toBytes("ZhangShan_" + i))
        put.addColumn(toBytes("info"), toBytes("age"), toBytes("19"))
        put.addColumn(toBytes("info"), toBytes("sex"), toBytes("男"))
        l.add(put)

        if ((i % 1000).equals(0)) {
          table.put(l)
          l = new ArrayList[Put]
        }
      }
      table.put(l)
    } catch {
      case e: TableNotFoundException => {
        println("不存在的表: " + tableName + "--->" + e.printStackTrace())
      }
      case t: Exception => {
        println(t.printStackTrace())
      }
    } finally {
      closeHTable(table)
    }

  }

  def updateUserInfo(tableName: String, rowKey: String): Unit = {
    putUserInfo(tableName, rowKey)
  }

  def scanTable(tableName: String, startRowKey: String, stopRowKey: String): Unit = {
    val table = getHTable(tableName)
    var result: ResultScanner = null
    try {
      val scan = new Scan(toBytes(startRowKey), toBytes(stopRowKey))
      scan.addColumn(toBytes("info"), toBytes("name"))
      scan.addColumn(toBytes("info"), toBytes("age"))
      scan.addColumn(toBytes("info"), toBytes("sex"))
      result = table.getScanner(scan)
//      result.forEach(row => {
//        //Bytes.toString(row.getRow)
//        val cells = row.rawCells()
//        cells.foreach(cell => {
//          print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":")
//          println(Bytes.toString(CellUtil.cloneQualifier(cell)) + "--->" + Bytes.toString(CellUtil.cloneValue(cell)))
//        })
//      }
//      )
    } catch {
      case e: TableNotFoundException => {
        println("不存在的表: " + tableName + "--->" + e.printStackTrace())
      }
      case t: Exception => {
        println(t.printStackTrace())
      }
    } finally {
      IOUtils.closeStream(result)
      IOUtils.closeStream(table)
      //closeHTable(table)
    }
  }

  def createTable(tableName: String): Unit = {
    val admin = conn.getAdmin()
    val htd = new HTableDescriptor(TableName.valueOf(tableName))
    val info = new HColumnDescriptor("info")
    info.setValue("VERSION", "3")
    htd.addFamily(info)
    val job = new HColumnDescriptor("job")
    job.setValue("VERSION", "3")
    htd.addFamily(job)
    if (admin.tableExists(TableName.valueOf(tableName))) {
      println("创建未成功。表已经存在: " + tableName)
    } else {
      admin.createTable(htd)
    }
  }

  def createTable2(tableName: String): Unit = {
    val admin = conn.getAdmin()
    val htd = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes())
      .setCompressTags(true)
      .setInMemoryCompaction(MemoryCompactionPolicy.ADAPTIVE)
      //.setMinVersions(2) //该行报表，暂未调查原因
      .setTimeToLive(60 * 60 * 24 * 7)
      .setValue("COMPRESSION", "SNAPPY")
      .setValue("hbase.hstore.engine.class", "org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine")
      .setValue("hbase.hstore.blockingStoreFiles", "60")
      .setValue("hbase.hstore.compaction.min", "2")
      .setValue("hbase.hstore.compaction.max", "60")
      .setCompactionCompressionType(Compression.Algorithm.SNAPPY)
      .build()

    val table = TableDescriptorBuilder
      .newBuilder(TableName.valueOf(tableName))
      .setMemStoreFlushSize(256 * 1024 * 1024)
      .setColumnFamily(htd)
      .setCompactionEnabled(true)
      .setMaxFileSize(1024 * 1024 * 1024)
      .setValue("COMPRESSION", "SNAPPY")
      .setValue("hbase.hstore.engine.class", "org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine")
      .setValue("hbase.hstore.blockingStoreFiles", "60")
      .setValue("hbase.hstore.compaction.min", "2")
      .setValue("hbase.hstore.compaction.max", "60")
      .setValue("BLOCKSIZE", "65536")
      //.setValue("VERSION", "3")
      .build()

    if (admin.tableExists(TableName.valueOf(tableName))) {
      println("创建未成功。表已经存在: " + tableName)
    } else {
      //val splitKeys = Array(Bytes.toBytes("1"), Bytes.toBytes("2"), Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5"), Bytes.toBytes("6"), Bytes.toBytes("7"), Bytes.toBytes("8"), Bytes.toBytes("9"))
      val splitKeys = {1 to 9}.map(num => toBytes(num.toString)).toArray
      admin.createTable(table, splitKeys)
      println("table has been create: " + tableName)
      admin.close()
    }
  }

  def dropTable(tableName: String): Unit = {
    val admin = conn.getAdmin()

    try {
      if (admin.tableExists(TableName.valueOf(tableName))) {
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
          admin.disableTable(TableName.valueOf(tableName))
        }
        admin.deleteTable(TableName.valueOf(tableName))
      }
      println("table has been deleted : " + tableName)
    } catch {
      case e: TableNotFoundException => {
        println("不存在的表: " + tableName + "--->" + e.printStackTrace())
      }
      case t: Exception => {
        println(t.printStackTrace())
      }
    } finally {
      admin.close()
    }
  }

  def toBytes(str: String) = {
    Bytes.toBytes(str)
  }

}
