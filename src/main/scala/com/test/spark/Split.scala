package com.test.spark

import org.apache.spark.sql.DataFrame

object Split {

  def split(input:String,out:String):Unit={
    val df = Utils.read(input)
    val names = df.select("name").distinct().collect().toArray.map(line=>line(0).toString)
    for(name<-names){
      val t = df.filter(df("name")===name)
      val out_path = out+"/"+name+"_res"
      Utils.write(t,out_path)
    }
  }
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val out = args(1)
    split(input,out)
    
  }

}
