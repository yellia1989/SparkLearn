package com.yk.sparkcore.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 分区内聚合和分区建聚合是统一个函数
    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 2)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    val result: RDD[(String, Int)] = rdd2.foldByKey(10)(_+_)
    result.foreach(x => print(x + "\t")) //(spark,1)	(hadooop,1)	(hello,2)
  }
}
