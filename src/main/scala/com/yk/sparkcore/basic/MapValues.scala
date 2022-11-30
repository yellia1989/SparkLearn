package com.yk.sparkcore.basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapValues {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mapValues").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    rdd2.mapValues(x => x + 1).foreach(x => print(x + "\t")) //(hello,2)	(hadooop,2)	(hello,2)	(spark,2)

  }
}
