package com.yk.sparkcore.basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("ahello", "bhadooop", "chello", "dspark", "chello", "chello"), 1)
    val rdd2: RDD[(String, Double)] = rdd1.map((_, Math.random() * 100 % 100))
    rdd2.sortByKey(true).foreach(x => print(x + "\t")) //(dspark,1)	(chello,1)	(bhadooop,1)	(ahello,1)
  }
}
