package com.test.spark



import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

import scala.util.Random
object KS {
  def cut(data:DataFrame,Part:Int=10,col:String):Array[Double]={
    val count = data.count().toInt
    if(count<=Part)
      return Array[Double]()
    var l = Math.ceil(count*1.0/Part).toInt
    var pos = List[Int](0,l)
    for(i<-Range(2,Part+1))
    {
        pos = pos:+Math.min(i*l,count-1)
    }
    var parts = List[Double]()
    val values = data.filter(data.col("index").isin(pos:_*)).select(col).collect().toArray.map(line=>line(0).toString.toDouble)
    values
  }

  def getKey(parts:Array[Double],value:Any,label:Int):(Double,Double,Int)={
    if(value == null || value=="nan")
      return (0,-1,label)
    var v  = value.toString.toDouble
    for(i<-parts.indices.drop(1)){
      if(v<=parts(i))
        return (parts(i-1),parts(i),label)
    }
    (0,-1,label)
  }
  def getAllCount(df:DataFrame,parts:Array[Double]):Map[(Double,Double,Int),Int]={
    var gs = Map[(Double,Double,Int),Int]()
    try {
      df.rdd.map(line => {
        val key = getKey(parts,line(1),line(0).toString.toInt)
        (key,1)
      }
      ).reduceByKey((x1,x2)=>x1+x2).collect().foreach(line=>{gs+=(line._1->line._2)})
    }catch {
      case e:Exception=>0
    }
    gs
  }
  def getIv(seq_good:Long,seq_bad:Long,total_good:Long,total_bad:Long): Double =
  {
    (seq_good*1.0/total_good-seq_bad*1.0/total_bad)*Math.log((seq_good*1.0/total_good)/(seq_bad*1.0/total_bad))
  }
  def getSeq(parts:Array[Double],index:Int,seq_good:Long,seq_bad:Long,total_good:Long,total_bad:Long,none_total_good:Long,none_total_bad:Long,acc_good:Long,acc_bad:Long): Array[Double]=
  {
    var res = List[Double]()
    res:+=index.toDouble
    res:+=parts(index-1).toDouble
    res:+=parts(index).toDouble
    res:+=(seq_bad+seq_good).toDouble
    res:+=seq_bad.toDouble
    res:+=seq_good.toDouble
    res:+=(seq_good+seq_bad)*1.0/(total_good+total_bad)
    res:+=(seq_bad)*1.0/(seq_good+seq_bad)
    res:+=acc_bad*1.0/total_bad
    res:+=acc_good*1.0/total_good
    res:+=Math.abs(acc_bad*1.0/total_bad-acc_good*1.0/total_good)
    res:+=getIv(seq_good,seq_bad,total_good+none_total_good,total_bad+none_total_bad)
    res.toArray
  }
  def KS(df:DataFrame,col:String):List[Array[Double]]={
    var map = Map[String,Any]()
    var res = List[Array[Double]]()
    var colName = Utils.tsCols(col)
    var all_data = Utils.tsCols(df.filter(df("label").isNotNull)).select("label",colName)

    var notnone_data = all_data.filter(all_data(colName).isNotNull)
    notnone_data = notnone_data.sort(colName)
    notnone_data = Utils.add_index(notnone_data)
    var parts = cut(notnone_data,10,colName)
    if(parts.length==0)
      return res
    var parts_count = getAllCount(all_data,parts)
    var Seq(none_total_good,none_total_bad)=Seq(0,0)
    var Seq(total_good,total_bad)=Seq(0,0)
    parts_count.foreach(line=>{
      if(line._1._3==0)
        if(line._1._1==0 && line._1._2== -1)
          none_total_good+=line._2
        else
          total_good+=line._2
      else
        if(line._1._1==0 && line._1._2== -1)
          none_total_bad+=line._2
        else
          total_bad+=line._2
    })

    if (total_bad==0 || total_bad == 0 || total_good+total_bad<=10)
      return res
    var Seq(acc_good,acc_bad)=Seq(0.toLong,0.toLong)
    for(index<-Range(1,parts.length)){
      val Seq(start,end)=Seq(parts(index-1),parts(index))
      var Seq(seq_good,seq_bad)=Seq(parts_count.getOrElse((start,end,1),0).toString.toInt,parts_count.getOrElse((start,end,1),0).toString.toInt)
      acc_good+=seq_good
      acc_bad+=seq_bad
      val seq_res = getSeq(parts,index,seq_good,seq_bad,total_good,total_bad,none_total_good ,none_total_bad,acc_good,acc_bad)
      res:+=seq_res
    }
    val head = List("seq","开始","结束","订单数","逾期数","正常用户数","百分比","逾期率","累计坏账户占比","累计好账户占比","KS","IV").mkString("\t")
    println(head)
    for(line<-res){
      println(line.mkString("\t"))
    }
    res
  }
  def main(args: Array[String]): Unit = {
    val path = args(0)
    var df = Utils.tsCols(Utils.read(path))
    df.cache()
    val features = df.columns.drop(5)
    features.map(line=>try{KS(df,line)}catch {case e:Exception=>0})
  }

}
