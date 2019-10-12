package com.test.spark
object Select {
  def main(args: Array[String]): Unit = {
    val spark = SparkEnv.getSession
    val ps = Utils.getPs(args)

    val input = ps.get("input").get(0)
    val out = ps.get("out").get(0)
    var df = Utils.read(input)
    if(ps.contains("repartions")){
      df = df.repartition(ps.get("repartions").get(0).toString.toInt)
    }
    if (ps.contains("featurelist")) {
      val featurelist = ps.get("featurelist").get(0)
      val fealistdf = Utils.read(featurelist)
      var fl = fealistdf.select("feature_name").collect().toArray.map(line => line(0).toString)
      fl = Utils.tsCols(fl, ".", "#")
      df = Utils.tsCols(df)
      if(ps.contains("common")){
        fl = fl.filter(line=>df.columns.contains(line))
      }
      df = df.select(fl.head, fl.tail: _*)
      df = Utils.rtsCols(df)
    }
    if(ps.contains("getSample")){
      var featurelist=Array[String]("name","idcard","phone","loan_dt","label").filter(line=>df.columns.contains(line))
      df = df.select(featurelist.head, featurelist.tail: _*)
    }
    if(ps.contains("whitelist")){
      val whitelist = ps.get("whitelist").get
      df = df.filter(df("name").isin(whitelist.toArray:_*))
    }
    if(ps.contains("blacklist")){
      val blacklist = ps.get("blacklist").get
      df = df.filter(df("name").isin(blacklist.toArray:_*)===false)
    }
    if(ps.contains("sample")){
      val sample_path = ps.get("sample").get(0)
      val sample = Utils.read(sample_path)
      df = Utils.join(df,sample,List(),"inner")
    }
    if(ps.contains("distinct")){
      var featurelist=Array[String]("name","idcard","phone","loan_dt","label").filter(line=>df.columns.contains(line))
      println("cols")
      featurelist.foreach(println)
      df = Utils.distict(df,featurelist)
    }
    Utils.write(df,out)
  }

}
