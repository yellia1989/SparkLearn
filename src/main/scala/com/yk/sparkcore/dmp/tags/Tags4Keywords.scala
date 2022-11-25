package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log
import org.apache.commons.lang.StringUtils

object Tags4Keywords extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      if (StringUtils.isNotEmpty(log.keywords)) {
        val fields: Array[String] = log.keywords.split("\\|").filter(keyword => {
          keyword.length >= 3 && keyword.length <= 8
        })
        fields.map(keyword => {
          map += ("K" + keyword.replace(":", "") -> 1)
        })
      }
    }

    map
  }
}
