package com.test.spark

object Replace {

  def to_float(s:String):String={
    try
      s.toFloat.toString
    catch {
      case e:Exception => ""
    }
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val ps = Utils.getPs(args)
    val sc = SparkEnv.getSc
    val input = ps.get("input").get
    val oldstr = ps.get("oldstr").getOrElse(List(""))
    val newstr = ps.get("newstr").getOrElse(List(""))(0)
    val model =  ps.get("type").getOrElse(List("col"))(0)
    var filelist = input.map(line=>sc.textFile(line))

    for(i<-Range(0,input.length)){
      var t = filelist(i)
      val meta = t.first().split("\t",0)
      if(model=="line")
        t = t.map(line=> {
          var res = line
          for(o<-oldstr){
            res = res.replace(o,newstr)
          }
          res
        }
        )
      else if(model=="col")
        t = t.map(line=>line.toString.split("\t",0).map(col=>if(oldstr.contains(col.toString))newstr else col).mkString("\t"))
      else if(model=="str2float"){
        var featurelist = ps.get("featurelist").get
        if(meta.contains(featurelist(0))==false){
          featurelist = Utils.readFeatureList(featurelist(0)).toList
        }
        val fi = featurelist.toArray.map(line=>meta.toList.indexOf(line))
        t = t.map(line=>
          {
            val ds = line.toString.split("\t",0)
            val res = ds
            for(c<-fi){
              val d = ds(c)
              if(featurelist.contains(d)==false)
                res(c)=to_float(d)
            }
            res.mkString("\t")
          })
      }

      val out = input(i)+"_r"
      Utils.hdfs_delete(out)
      t.saveAsTextFile(out)
      if(ps.contains("cover"))
        Utils.hdfs_rename(out,input(i))
    }
  }

}
