package com.yk.sparkcore.com.yk.sparkcore.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    import spark.implicits._

    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd: RDD[(String, String)] = spark.sparkContext.parallelize(data)

    val dfFromRDD1 = rdd.toDF("language","users_count")
    dfFromRDD1.printSchema()

    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRDD2.printSchema()

    //From RDD (USING createDataFrame and Adding schema using StructType)
    val schema = StructType(columns
      .map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //convert RDD[T] to RDD[Row]
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
    val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)
    dfFromRDD3.printSchema()

    val ds = spark.createDataset(rdd)
    ds.printSchema()

    val dfCsv1: DataFrame = spark.read.option("header", true).csv("data/csv/text01.csv", "data/csv/text02.csv")
    dfCsv1.printSchema()
    dfCsv1.show()

    spark.close()
  }
}
