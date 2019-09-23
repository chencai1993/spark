package com.test.spark

object Merge {

  def main(args: Array[String]): Unit = {

    //val test = Array[String]("--input","test","test2","--out","test3","--joinType","inner","--r")
    val ps = Utils.getPs(args)
    val out =  ps.get("out").get(0)
    val joinType = ps.get("joinType").get(0)
    var filepathlist =  ps.get("input").get
    val replace = ps.contains("replace") || ps.contains("r")
    var filelist = filepathlist.map(line=>Utils.tsCols(Utils.read(line)))
    var res = filelist(0)
    for(f<-filelist.drop(1))
    {
        if(replace)
          {
            val commonfeatures = Utils.commonColsNoName(res,f)
            res = res.drop(commonfeatures:_*)
          }
        res = Utils.join(res,f,List(),joinType)
    }
    res = Utils.rtsCols(res)
    Utils.write(res,out)
  }

}
