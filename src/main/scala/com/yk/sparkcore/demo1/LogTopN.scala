package com.yk.sparkcore.demo1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 统计访问次数top2的网页
 */

object LogTopN {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val context: SparkContext = session.sparkContext

    val logRDD: RDD[String] = context.textFile("data/logtopn/log.txt")

    val pathRDD: RDD[(String, Int)] = logRDD.map {
      line => {
        val fields: Array[String] = line.split(" ")
        if (fields.length != 10) {
          ("", 1)
        } else {
          (fields(6), 1)
        }
      }
    }.filter(_._1.nonEmpty)

    val tuples: Array[(String, Int)] = pathRDD.reduceByKey(_ + _).sortBy(_._2, false).take(2)
    tuples.foreach(println(_))

    session.stop()
  }
}
