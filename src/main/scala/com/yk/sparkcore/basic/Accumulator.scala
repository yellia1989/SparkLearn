package com.yk.sparkcore.basic

import org.apache.spark.sql.SparkSession

object Accumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local")
      .getOrCreate()

    val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3))

    rdd.foreach(x => longAcc.add(x))

    println(longAcc.value)
  }
}
