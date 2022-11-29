package com.yk.sparkcore.jdbc

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, ResultSet}

case class Dept(no:Int, name:String, loc:String)

object Jdbc {
  def main(args: Array[String]): Unit = {

    // load and register JDBC driver for MySQL
    //Class.forName("com.mysql.jdbc.Driver");

    val getConn: () => Connection = () => {
      DriverManager.getConnection("jdbc:mysql://bigdata03:3306/sqoop?characterEncoding=UTF-8&useSSL=false", "root", "bigdata")
    }

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
      .setAppName(this.getClass.getSimpleName)

    val context: SparkContext = new SparkContext(conf)

    val jdbcRDD: JdbcRDD[Dept] = new JdbcRDD[Dept](context,
      getConn,
      "select deptno,dname,loc from dept where deptno > ? and deptno < ?",
      0,
      100,
      1,
      rs => {
        Dept(rs.getInt("deptno"), rs.getString("dname"), rs.getString("loc"))
      })

    val rdd2: RDD[Dept] = jdbcRDD.filter(_.no >= 30)

    //rdd2.foreach(println)

    rdd2.foreachPartition{
      iter => {
        val conn: Connection = getConn()
        conn.setAutoCommit(false)

        val statement = conn.prepareStatement("insert into dept_copy(deptno, dname, loc) values (?, ?, ?)")

        iter.foreach{
          dept => {
            println(dept)
            statement.setInt(1, dept.no)
            statement.setString(2, dept.name)
            statement.setString(3, dept.loc)
            statement.addBatch()
          }
        }

        statement.executeBatch()
        conn.commit()
        conn.close()

        statement.close()
      }
    }

    context.stop()
  }
}
