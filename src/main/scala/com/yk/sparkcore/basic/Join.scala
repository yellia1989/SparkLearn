package com.yk.sparkcore.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("join").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a",10),("b",10),("a",20),("d",10)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a",30),("b",20),("c",10)))


    //相当于mysql的INNER JOIN，当join左右两边的数据集都存在时才返回
    //内连接 (a,(10,30))	(b,(10,20))	(a,(20,30))
    rdd1.join(rdd2).foreach(x => print(x + "\t"))
    println()

    //相当于mysql的LEFT JOIN，leftOuterJoin返回数据集左边的全部数据和数据集左边与右边有交集的数据
    //左链接(b,(10,Some(20)))	(d,(10,None))	(a,(10,Some(30)))	(a,(20,Some(30)))
    rdd1.leftOuterJoin(rdd2).foreach(x => print(x + "\t"))
    println()

    //相当于mysql的RIGHT JOIN，rightOuterJoin返回数据集右边的全部数据和数据集右边与左边有交集的数据
    //右链接(c,(None,10))	(a,(Some(10),30))	(b,(Some(10),20))	(a,(Some(20),30))
    rdd1.rightOuterJoin(rdd2).foreach(x => print(x + "\t"))
    println()

    //返回左右数据集的全部数据，左右有一边不存在的数据以None填充
    //全链接(b,(Some(10),Some(20)))	(c,(None,Some(10)))	(d,(Some(10),None))	(a,(Some(10),Some(30)))	(a,(Some(20),Some(30)))
    rdd1.fullOuterJoin(rdd2).foreach(x => print(x + "\t"))
  }
}
