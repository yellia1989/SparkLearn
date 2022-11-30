package com.yk.sparkcore.basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CombineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("combineByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("A", 2), ("B", 1), ("A", 1), ("A", 3), ("C", 1)), 2)
    rdd1.mapPartitionsWithIndex {
      case (index, iter) => {
        iter.map {
          case (p1, p2) => {
            (index + "_" + p1, p2)
          }
        }
      }
    }.foreach(println)

    rdd1.combineByKey(
      (v:Int) => v + "_",
      (c1:String, c2:Int) => c1 + "@" + c2, // 统一分区内
      (c1:String, c2:String) => c1 + "|" + c2 // 结果合并
    ).foreach(println)

    sc.stop()
  }
}
