package com.yk.sparkcore.com.yk.sparkcore.basic

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CSV {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Parallelize")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    println("spark read csv files from a directory into RDD")
    val rddFromFile = sc.textFile("data/csv/text01.csv")
    println(rddFromFile.getClass)

    val rdd = rddFromFile.map(f=>{
      f.split(",")
    })

    println("Iterate RDD")
    rdd.foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })
    println(rdd)

    println("Get data Using collect")
    rdd.collect().foreach(f=>{
      println("Col1:"+f(0)+",Col2:"+f(1))
    })

    println("read all csv files from a directory to single RDD")
    val rdd2 = sc.textFile("data/csv/*")
    rdd2.foreach(f=>{
      println(f)
    })

    println("read csv files base on wildcard character")
    val rdd3 = sc.textFile("data/csv/text*.csv")
    rdd3.foreach(f=>{
      println(f)
    })

    println("read multiple csv files into a RDD")
    val rdd4 = sc.textFile("data/csv/text01.csv,data/csv/text02.csv")
    rdd4.foreach(f=>{
      println(f)
    })
  }
}
