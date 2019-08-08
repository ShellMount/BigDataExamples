package com.setapi.api.hdfs

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
  *
  * Created by ShellMount on 2019/7/3
  *
  **/

class HdfsApi {
  var fs: FileSystem = _

  def getHadoopFileSystem {
    var conf: Configuration = null
    conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://192.168.0.211:9000")

    try {
      fs = FileSystem.get(conf)
      //fs = FileSystem.newInstance(conf)
      println("Where is here")
    } catch {
      case e: IOException => println("---> " + e.printStackTrace())
      case f: Throwable => println("---> There is a Error")
    } finally {
      println("---> FS GOT or did not? : " + fs)
    }
  }

  /**
    * @return
    */
  def createDirectory(): Boolean = {
    var b = false
    val path = new Path("/DfsRoot/NewDirectory")
    try {
      b = fs.mkdirs(path)
    } catch {
      case e: IOException => {e.printStackTrace()}
    } finally {
      //fs.close()
    }
    return b
  }

  def rmDirectory(): Boolean = {
    var b = false
    val path = new Path("/DfsRoot/NewDirectory")
    try {
      b = fs.delete(path, true)
    } finally {
      //fs.close()
    }
    return b
  }

  def rename(): Unit = {
    var b: Boolean = false

  }

  def putFile2HDFS(): Unit = {
    var pathExisted: Boolean = false
    val localPath = new Path("E:\\APP\\Tmp\\HDFS\\localfile.txt")
    val hdfsPath = new Path("/DfsRoot/NewDirectory/hdfsfile.txt")

    fs.copyFromLocalFile(localPath, hdfsPath)
  }

  def getFileFromHDFS: Unit = {
    val localPath = new Path("E:\\APP\\Tmp\\HDFS\\comefome_hdfs.txt")
    val hdfsPath = new Path("/DfsRoot/NewDirectory/hdfsfile.txt")

    fs.copyToLocalFile(hdfsPath, localPath)
  }

  def copyBetweenHDFS: Unit = {
    val inPath = new Path("/DfsRoot/NewDirectory/hdfsfile.txt")
    val outPath = new Path("/DfsRoot/NewDirectory/hdfsfile_copy.txt")
    var hdfsOut: FSDataOutputStream = null
    var hdfsIn: FSDataInputStream = null

    hdfsIn = fs.open(inPath)
    hdfsOut = fs.create(outPath)

    IOUtils.copyBytes(hdfsIn, hdfsOut, 1024*1024*64, false)
  }
}
