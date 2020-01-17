package com.test.spark

object Union {
  def main(args: Array[String]): Unit = {
    val ps = Utils.getPs(args)

    val featurelist = Utils.tsCols(Utils.readFeatureList(ps.get("featurelist").get(0)))
    val out = ps.get("out").get(0)
    val input = ps.get("input").get.map(line=>Utils.tsCols(Utils.read(line)).select(featurelist.head,featurelist.tail:_*))
    var res = input(0)
    for(df<-input.slice(1,input.length)){
      res = res.union(df)
    }
    res = Utils.rtsCols(res)
    Utils.write(res,out)

  }

}
