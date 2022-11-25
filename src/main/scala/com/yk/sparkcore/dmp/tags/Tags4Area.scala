package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log
import org.apache.commons.lang.StringUtils

object Tags4Area extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      if(StringUtils.isNotEmpty(log.provincename)){
        map += ("ZP"+log.provincename ->1)
      }
      if(StringUtils.isNotEmpty(log.cityname)){
        map += ("ZC" +log.cityname ->1)
      }
    }

    map
  }
}
