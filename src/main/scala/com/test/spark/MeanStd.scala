package com.test.spark

object MeanStd {
  def main(args: Array[String]): Unit = {
    val ps = Utils.getPs(args)
    val input = ps.get("input").get(0)
    val out = ps.get("out").get(0)
    var df = Utils.read(input)
    val drop_cols=Array("name","idcard","phone","loan_dt","label").filter(line=>df.columns.contains(line))
    df = df.drop(drop_cols:_*)
    val res  = df.describe()
    Utils.write(res,out,1)
  }

}
