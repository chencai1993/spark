package com.test.spark

object Select {
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val input = args(0)
    val featurelist = args(1)
    val out = args(2)
    var df = Utils.read(spark,input)
    val fealistdf = Utils.read(spark,featurelist)
    var fl = fealistdf.select("feature_name").collect().toArray.map(line=>line(0).toString)
    fl = Utils.tsCols(fl,".","#")
    df = Utils.tsCols(df).select(fl.head,fl.tail: _*)
    df = Utils.rtsCols(df)
    Utils.write(df,out)
  }

}
