package com.yk.sparkgraphx

import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 图计算好友关系
 */
object FindFriend {
  def main(args: Array[String]): Unit = {
    //graphx 基于RDD
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    //构建出来图有多种方式
    val grapxh = GraphLoader.edgeListFile(sc,"data/graphx/followers.txt")
    grapxh.vertices.foreach(println(_))
    grapxh.edges.foreach(println)

    /**
     * (4,1)
        (1,1)
       (6,1)
      (3,1)
     (7,1)
    (5,1)
    (2,1)
     */

    val cc = grapxh.connectedComponents().vertices
    cc.foreach(println)

    /**
     *
     * (4,4)
     *(6,4)
     *(7,4)
     **
       (1,1)
     *(3,1)
     *(5,1)
     *(2,1)
     *
     */
    val users = sc.textFile("data/graphx/users.txt").map(line => {
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    })

    //1,刘德华 join   1,1
    //1 刘德华，1（代表的是同一个好友的那个id）
    val joinRDD: RDD[(VertexId, String)] = users.join(cc).map {
      case (id, (username, ccminid)) => (ccminid, username)
    }
    joinRDD.foreach(println)

    joinRDD.reduceByKey( (x:String,y:String) => x + ","+ y)
      .foreach(tuple =>{
        println(tuple._2)
      })


    sc.stop()
  }

}