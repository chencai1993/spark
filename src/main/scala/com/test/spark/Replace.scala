package com.test.spark

object Replace {
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val ps = Utils.getPs(args)
    val sc = SparkEnv.getSc
    val input = ps.get("input").get
    val oldstr = ps.get("oldstr").get(0)
    val newstr = ps.get("newstr").get(0)
    var filelist = input.map(line=>sc.textFile(line))
    for(i<-Range(0,input.length)){
      var t = filelist(i)
      t = t.map(line=>line.replace(oldstr,newstr))
      val out = input(i)+"_r"
      t.saveAsTextFile(out)
    }
  }

}
