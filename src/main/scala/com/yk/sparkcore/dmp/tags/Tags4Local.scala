package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log

object Tags4Local extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      if(log.adspacetype !=0 && log.adspacetype != null){
        log.adspacetype match {
          case x if x<10 => map += ("LC0" +x ->1)
          case x if x>9 =>map += ("LC" +x ->1)
        }
      }
    }
    map
  }
}
