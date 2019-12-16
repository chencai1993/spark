package com.test.spark

object Merge {

  def main(args: Array[String]): Unit = {

    //val test = Array[String]("--input","test","test2","--out","test3","--joinType","inner","--r")
    val ps = Utils.getPs(args)
    val out =  ps.get("out").get(0)
    val joinType = ps.get("joinType").getOrElse(List("inner"))(0)
    var filepathlist =  ps.get("input").get
    val replace = ps.contains("replace") || ps.contains("r")
    var filelist = filepathlist.map(line=>Utils.read(line))
    if(replace){
      filelist = filelist.map(line=>Utils.tsCols(line))
    }
    if(ps.contains("repartions")){
      val repartions = ps.get("repartions").get(0).toInt
      filelist = filelist.map(line=>line.repartition(repartions))
    }
    var res = filelist(0)
    for(f<-filelist.drop(1))
    {
        if(replace)
          {
            val commonfeatures = Utils.commonColsNoName(res,f)
            if(commonfeatures.length>0)
                res = res.drop(commonfeatures:_*)
          }
        res = Utils.join(res,f,List(),joinType)
    }
    res = Utils.rtsCols(res)
    Utils.write(res,out)
  }

}
