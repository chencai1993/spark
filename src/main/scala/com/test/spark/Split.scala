package com.test.spark

import org.apache.spark.sql.DataFrame
object Split {
  def main(args: Array[String]): Unit = {
    val ps = Utils.getPs(args)
    val input = ps.get("input").get(0)
    val data = Utils.read(input)
    val splitfiles = ps.get("files").get
    for(f<-splitfiles){
      val file = Utils.read(f)
      val t = Utils.join(data,file)
      Utils.write(t,f+"_merge")
    }
  }
}
