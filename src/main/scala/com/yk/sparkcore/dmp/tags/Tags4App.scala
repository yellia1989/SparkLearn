package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log
import org.apache.commons.lang.StringUtils

object Tags4App extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    if (args.length > 1) {
      val log: Log = args(0).asInstanceOf[Log]
      val appDict = args(1).asInstanceOf[Map[String, String]]
      val appName = appDict.getOrElse(log.appid, log.appname)
      if(StringUtils.isNotEmpty(appName)){
        map += ("APP" + appName ->1)
      }
    }

    map
  }
}
