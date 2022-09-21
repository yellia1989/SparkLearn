package com.yk.sparkcore

import org.apache.spark.SparkContext

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext("local", "HelloWorld")

    sc.textFile("data/test.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).foreach(println)

    sc.stop()
  }
}
