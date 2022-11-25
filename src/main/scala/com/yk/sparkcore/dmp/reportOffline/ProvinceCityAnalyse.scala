package com.yk.sparkcore.dmp.reportOffline

import com.yk.sparkcore.dmp.utils.Utils
import org.apache.spark.sql.{DataFrame, SparkSession}

// 每个省每个城市有多少条数据
object ProvinceCityAnalyse {
  def main(args: Array[String]): Unit = {
    // 读取参数
    if (args.length != 2) {
      println(
        """
          | inputPath outputPath
          |   inputPath 输入路径，parquet格式
          |   outputPath 输出路径
          |""".stripMargin)
      System.exit(0)
    }
    var Array(inputPath, outputPath) = args

    Utils.deleteFile(outputPath)

    // 创建执行环境
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // 读取文件
    val logDf: DataFrame = session.read.parquet(inputPath)
    logDf.createOrReplaceTempView("logs")

    val resultDf: DataFrame = session.sql(
      """
        |select provincename,cityname,count(*) as count from logs group by provincename,cityname
        |""".stripMargin).repartition(1)

    resultDf.write.json(outputPath)

    // 释放
    session.close()
  }
}
