package com.yk.sparkcore.dmp.utils

import org.apache.commons.lang.StringUtils

case class A(name:String)
class B(name:String)

object Utils {
  def parseInt(v:String):Int = {
    try {
      v.isEmpty match {
        case true => 0
        case false => v.toInt
      }
    } catch {
      case _ => 0
    }
  }

  def parseDouble(v:String):Double = {
    try {
      v.isEmpty match {
        case true => 0.0
        case false => v.toDouble
      }
    } catch {
      case _ => 0.0
    }
  }

  def fmtDate(v:String):Option[String] = {
    val fields: Array[String] = v.split(" ")
    if (fields.length != 2) return None
    Some(fields(0).replace("-", ""))
  }

  def fmtHour(v:String):Option[String] = {
    val fields: Array[String] = v.split(" ")
    if (fields.length != 2) return None
    Some(fields(1).substring(0, 2))
  }

  def main(args: Array[String]): Unit = {

    println(parseInt(null))
    println(parseInt("a"))
    println(parseInt("1"))
    println(parseDouble("0.99"))

    println(fmtDate("2010-12-12 12:11:11").getOrElse("unknown"))
    println(fmtDate("2011").getOrElse("unknown"))

    println(fmtHour("2012-12-12 12:11:11").getOrElse("unknown"))
  }
}
