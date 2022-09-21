package com.yk.sparkcore.com.yk.sparkcore.basic

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Parallelize {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Parallelize")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 4, 5))
    rdd1.collect().foreach(println)

    sparkSession.stop()
  }
}
