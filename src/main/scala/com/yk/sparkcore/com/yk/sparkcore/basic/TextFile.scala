package com.yk.sparkcore.com.yk.sparkcore.basic

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TextFile {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Parallelize")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    println("read all text files from a directory to single RDD")
    val rdd = sc.textFile("data/textfile/*")
    rdd.foreach(f=>{
      println(f)
    })

    println("read text files base on wildcard character")
    val rdd2 = sc.textFile("data/textfile/text*.txt")
    rdd2.foreach(f=>{
      println(f)
    })

    println("read multiple text files into a RDD")
    val rdd3 = sc.textFile("data/textfile/text01.txt,data/textfile/text02.txt")
    rdd3.foreach(f=>{
      println(f)
    })

    println("Read files and directory together")
    val rdd4 = sc.textFile("data/textfile/text01.txt,data/textfile/text02.txt,data/textfile/*")
    rdd4.foreach(f=>{
      println(f)
    })

    val rddWhole = sc.wholeTextFiles("data/textfile/*")
    rddWhole.foreach(f=>{
      println(f._1+"=>"+f._2)
    })

    val rdd5 = sc.textFile("data/textfile/*")
    val rdd6 = rdd5.map(f=>{
      f.split(",")
    })

    rdd6.foreach(f => {
      println("Col1:"+f(0)+",Col2:"+f(1))
    })
  }
}
