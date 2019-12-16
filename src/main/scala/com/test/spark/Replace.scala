package com.test.spark

object Replace {
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val ps = Utils.getPs(args)
    val sc = SparkEnv.getSc
    val input = ps.get("input").get
    val oldstr = ps.get("oldstr").get
    val newstr = ps.get("newstr").getOrElse(List(""))(0)
    val model =  ps.get("type").getOrElse(List("col"))(0)
    var filelist = input.map(line=>sc.textFile(line))
    for(i<-Range(0,input.length)){
      var t = filelist(i)
      if(model=="line")
        t = t.map(line=> {
          var res = line
          for(o<-oldstr){
            res = res.replace(o,newstr)
          }
          res
        }
        )
      else
        t = t.map(line=>line.toString.split("\t",0).map(col=>if(oldstr.contains(col))newstr else col).mkString("\t"))
      val out = input(i)+"_r"
      Utils.hdfs_delete(out)
      t.saveAsTextFile(out)
      if(ps.contains("cover"))
        Utils.hdfs_rename(out,input(i))
    }
  }

}
