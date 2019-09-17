package com.test.spark

object Merge {

  def main(args: Array[String]): Unit = {

    val spark = SparkEnv.getSession
    val out =  args(0)
    val joinType = args(1)
    var filepathlist =  args.drop(2)
    var filelist = filepathlist.map(line=>Utils.read(spark,line))
    var res = filelist(0)
    for(f<-filelist.drop(1))
    {
        res = Utils.join(res,f,List(),joinType)
    }
    Utils.write(res,out)
  }

}
