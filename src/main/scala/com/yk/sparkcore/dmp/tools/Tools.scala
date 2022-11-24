package com.yk.sparkcore.dmp.tools

import com.yk.sparkcore.dmp.beans.Log
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object Tools {
  def main(args: Array[String]): Unit = {
    // 判断参数个数
    if (args.length != 3) {
      println(
        """
          | outPutPath inputPath compressCodec
          |     outPutPath 输出路径
          |     intPutPath 输入路径
          |     compressCodec 压缩方式
          |""".stripMargin)
      System.exit(0)
    }

    // 接收参数
    var Array(outputPath, inputPath, compressCodec) = args

    val file = new File(outputPath)
    file.isDirectory match {
      case false => file.delete()
      case true => {
        for (f <- file.listFiles()) {
          f.delete()
        }
        file.delete()
      }
    }


    // 创建执行环境
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    conf.setMaster("local")
    conf.set("spark.seriallizer", "org.apache.spark.seriallizer.KryoSeriallizer")
    conf.set("spark.sql.parquet.compression.codec", "gzip")
    conf.registerKryoClasses(Array(classOf[Log]))

    val session: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // 处理
    val textRDD: RDD[String] = session.sparkContext.textFile(inputPath)
    val logRDD: RDD[Log] = textRDD.map(Log.line2Log(_))

    val logDF: DataFrame = session.createDataFrame(logRDD)
    logDF.write.parquet(outputPath)

    // 关闭
    session.close()
  }
}
