package com.yk.sparkcore.dmp.tools

import com.yk.sparkcore.dmp.utils.Utils
import org.apache.spark.graphx.{Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class User(id:Int, device:Set[String], keywords:Set[String])

object Users {
  def main(args: Array[String]): Unit = {
    // 解析参数
    if (args.length !=2 ) {
      println(
        """
          | inputPath outputPath
          |   inputPath 用户标签文件
          |   outputPath 最终用户标签文件
          |""".stripMargin)
      System.exit(0)
    }

    val Array(inputPath,outputPath) = args
    Utils.deleteFile(outputPath)

    // 创建执行环境
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    val context: SparkContext = new SparkContext(conf)

    val usersRDD: RDD[(Long,User)] = context.textFile(inputPath).map {
      line => {
        val fields: Array[String] = line.split(" ")
        if (fields.length != 3) {
          (0L,null)
        } else {
          (fields(0).toLong, User(fields(0).toInt, fields(1).split(",").toSet, fields(2).split(",").toSet))
        }
      }
    }.filter(_._1 != 0).cache()

    // 将同设备的用户分组到一起
    val device2UseridRDD: RDD[(String, Iterable[Int])] = usersRDD.flatMap {
      case (_,user) => {
        var device2UserId: Map[String, Int] = Map[String, Int]()
        user.device.map {
          device => device2UserId += (device -> user.id)
        }
        device2UserId.iterator
      }
    }.groupByKey()

    // 生成follow关系
    val useridsRDD: RDD[String] = device2UseridRDD.flatMap {
      case (_, iterator) => {
        var ids: Set[String] = Set[String]()
        val list: List[Int] = iterator.toList
        list.length match {
          case 1 => ids += (list(0).toString + " " + list(0).toString)
          case _ => list.map {
            userid => {
              ids += (list(0).toString + " " + userid.toString)
            }
          }
        }
        ids.iterator
      }
    }
    useridsRDD.saveAsTextFile(outputPath)

    // 构造设备连通子图
    val graphx: Graph[Int, Int] = GraphLoader.edgeListFile(context, outputPath)
    val cc = graphx.connectedComponents().vertices

    // 合并标签和设备
    usersRDD.join(cc).map {
      case (_, (user, minuserid)) => {
        (minuserid, (user.keywords, user.device))
      }
    }.reduceByKey {
      case ((keys1,devices1), (keys2,devices2)) => {
        (keys1 ++ keys2, devices1 ++ devices2)
      }
    }.foreach(println)

    // 释放
    context.stop()
  }
}
