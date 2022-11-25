package com.yk.sparkcore.dmp.tags

import com.yk.sparkcore.dmp.beans.Log
import com.yk.sparkcore.dmp.utils.Utils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TagContext {
  def main(args: Array[String]): Unit = {
    // 接收参数
    if (args.length != 4) {
      println(
        """
          | inputPath mappingPath outputPath
          |   inputPath 日志文件
          |   appmapping app映射文件
          |   devicemapping 设备映射文件
          |   outputPath 输出目录
          |""".stripMargin)
      System.exit(0)
    }
    val Array(inputPath, appPath, devicePath, outputPath) = args
    Utils.deleteFile(outputPath)

    // 创建环境
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Log]))

    val session: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // 创建app广播变量
    val appMap: Map[String, String] = session.sparkContext.textFile(appPath).map {
      case line => {
        val fields: Array[String] = line.replace(" ","").split("\t")
        if (fields.length > 4) {
          (fields(4).trim, fields(1).trim)
        } else {
          ("", "")
        }
      }
    }.filter(v => StringUtils.isNotEmpty(v._1)).collect().toMap
    val appBroadcast: Broadcast[Map[String, String]] = session.sparkContext.broadcast(appMap)

    // 创建设备广播变量
    val deviceMap: Map[String, String] = session.sparkContext.textFile(devicePath).map(line => {
      val fields: Array[String] = line.split("\t")
      if (fields.length > 2) {
        (fields(0).trim, fields(2).trim)
      } else {
        ("", "")
      }
    }).filter(v => StringUtils.isNotEmpty(v._1)).collect().toMap
    val deviceBroadcast: Broadcast[Map[String, String]] = session.sparkContext.broadcast(deviceMap)

    val logRDD: RDD[(String, List[(String, Int)])] = session.sparkContext.textFile(inputPath).map({
      case line => {
        val log: Log = Log.line2Log(line)

        val localTags = Tags4Local.make(log)
        val appTags = Tags4App.make(log, appBroadcast.value)
        val channelTags = Tags4Channel.make(log)
        val deviceTags = Tags4Device.make(log, deviceBroadcast.value)
        val keywordsTags = Tags4Keywords.make(log)
        val areaTags = Tags4Area.make(log)
        val tags = localTags ++ appTags ++ channelTags ++ deviceTags ++ keywordsTags ++ areaTags

        // 打标签
        var userid = getUUID(log).getOrElse("")
        (userid, tags.toList)
      }
    }).filter(!_._1.isEmpty)

    logRDD.reduceByKey{
      case (list1,list2) => {
        (list1 ++ list2).groupBy(_._1).map({
          case (keyword,list) => {
            (keyword, list.map(_._2).sum)
          }
        }).toList
      }
    }.collect().foreach(println)

    // 释放资源
    session.close()
  }

  def getUUID(log:Log):Option[String] = log match {
  case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
  case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
  case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

  case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
  case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
  case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

  case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
  case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
  case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

  case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
  case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
  case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

  case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
  case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
  case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

  case _ => None
  }
}
