package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log

object Tags4Device extends Tags {
  override def make(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()

    if (args.length > 1) {
      val log: Log = args(0).asInstanceOf[Log]
      val deviceDict = args(1).asInstanceOf[Map[String,String]]

      //操作系统标签
      val os = deviceDict.getOrElse(log.client.toString, deviceDict.get("4").get)
      map += (os -> 1)
      //联网方式
      val netWork = deviceDict.getOrElse(log.networkmannername, deviceDict.get("NETWORKOTHER").get)
      map += (netWork -> 1)
      //运营商方案
      val isp = deviceDict.getOrElse(log.ispname, deviceDict.get("OPERATOROTHER").get)
      map += (isp ->1)
    }

    map
  }
}
