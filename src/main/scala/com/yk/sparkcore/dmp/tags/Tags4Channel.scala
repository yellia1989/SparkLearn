package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log
import org.apache.commons.lang.StringUtils

object Tags4Channel extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      if (StringUtils.isNotEmpty(log.channelid)) {
        map += ("CN" + log.channelid -> 1)
      }
    }

    map
  }
}
