package com.test.spark


object JsonToDF {
  def main(args: Array[String]): Unit = {
    val ps = Utils.getPs(args)
    val spark = SparkEnv.getSession
    val input = Utils.read(ps.get("input").get(0))
    val jsoncol = ps.get("feature").get(0)
    val featurelist = Utils.readFeatureList(ps.get("featurelist").get(0))
    val out = ps.get("out").get(0)
    var res = input.drop(jsoncol)
    val datas = input.select(jsoncol).rdd.map(line=>line(0).toString.replace(": Nan",": \"\"").replace("\"nan\"","\"\""))
    val features = spark.read.json(datas).select(featurelist.head,featurelist.tail:_*)
    res = Utils.concat(res,features)
    Utils.write(res,out)
  }
}
