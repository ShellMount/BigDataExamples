package com.setapi.api.hive

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text


/**
  *
  * Created by ShellMount on 2019/7/13
  *
  **/

class HiveApi extends UDF{
  def lower(str: String): Text = {
    if (StringUtils.isEmpty(str)) return null

    return new Text(str.toLowerCase())
  }

  def evaluate(str: String): Text = {
    return lower(str)
  }

}
