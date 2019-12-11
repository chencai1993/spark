package com.test.spark

object Replace {
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val ps = Utils.getPs(args)
    val sc = SparkEnv.getSc
    val input = Seq("test")//ps.get("input").get
    val oldstr = "DL"//ps.get("oldstr").get(0)
    val newstr = "" //ps.get("newstr").get(0)
    val model =  ps.getOrElse("model","col")
    var filelist = input.map(line=>sc.textFile(line))
    for(i<-Range(0,input.length)){
      var t = filelist(i)
      if(model=="line")
        t = t.map(line=>line.replace(oldstr,newstr))
      else
        t = t.map(line=>line.toString.split("\t",0).map(col=>if(col==oldstr)newstr else col).mkString("\t"))
      val out = input(i)+"_r"
      t.saveAsTextFile(out)
    }
  }

}
