package com.yk.sparkcore.com.yk.sparkcore.basic

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object PrintSchema {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getName)
      .getOrCreate()

    import spark.implicits._

    // Example 3 - Nested structure
    // Create Nested Structure
    val schema_nest = StructType( Array(
      StructField("name",StructType( Array(
        StructField("firstname", StringType,true),
        StructField("middlename", StringType,true),
        StructField("lastname", StringType,true)
      ))),
      StructField("language", StringType,true),
      StructField("fee", IntegerType,true)
    ))

    //Create DataFrame
    val data3 = List(
      Row(Row("James","","Smith"),"Java", "20000"),
      Row(Row("Michael","Rose",""),"Python", "100000")
    )
    val df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(data3),schema_nest)
    df3.printSchema()

    //Use ArrayType & MapType
    import org.apache.spark.sql.types.{ArrayType,MapType}
    val schema_col = StructType( Array(
      StructField("name", StringType,true),
      StructField("languages", ArrayType(StringType),true),
      StructField("properties", MapType(StringType,StringType),true)
    ))

    //Create DataFrame
    val data4 = List(
      Row("James",List("Java","Scala"), Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Python","PHP"), Map("hair"->"brown","eye"->"black"))
    )
    val df4 = spark.createDataFrame(
      spark.sparkContext.parallelize(data4),schema_col)
    df4.printSchema()

    spark.close()
  }
}
