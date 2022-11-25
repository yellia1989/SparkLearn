package com.yk.sparkcore.dmp.tags

trait Tags {
  def make(args:Any*):Map[String,Int]
}
