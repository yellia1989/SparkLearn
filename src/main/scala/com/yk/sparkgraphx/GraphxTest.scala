package com.yk.sparkgraphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object GraphxTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("GraphTest")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    //构建顶点
    val users: RDD[(Long, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    //构建边
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    //顶点和边    rowRDD和Schema
    val graph = Graph(users,relationships)

    val count = graph.vertices.filter {
      case (id, (name, pos)) => {
        pos == "postdoc"
      }
    }.count()
    println(count)

    val count1 = graph.edges.filter(e => e.srcId > e.dstId).count()
    println(count1)

    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(println(_))


    sc.stop()
  }
}
