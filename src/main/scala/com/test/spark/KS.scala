package com.test.spark



import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

import scala.util.Random
object KS {
  def cut(data:DataFrame,Part:Int=10,col:String):Array[Double]={
    val count = data.count().toInt
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

  def getKey(parts:Array[Double],value:Double,label:Int):(Double,Double,Int)={
    for(i<-parts.indices.drop(1)){
      if(value<=parts(i))
        return (parts(i-1),parts(i),label)
    }
    (-1,-1,label)
  }
  def getAllCount(df:DataFrame,parts:Array[Double]):Map[(Double,Double,Int),Int]={
    var gs = Map[(Double,Double,Int),Int]()
    try {
      val t = df.rdd.map(line => {
        val key = getKey(parts,line(1).toString.toDouble,line(0).toString.toInt)
        (key,1)
      }
      ).reduceByKey((x1,x2)=>x1+x2).collect().foreach(line=>gs+=(line._1->line._2))
    }catch {
      case e:Exception=>0
    }
    gs
  }
  def getCount(df:DataFrame):Map[Int,Long]={
    var gs = (0.toInt,0.toInt)
    try {
       gs = df.rdd.map(line => {
         if(line(0).toString=="0")
           (1,0)
         else
           (0,1)
      }
      ).reduce((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))
    }catch {
      case e:Exception=>gs = (0.toInt,0.toInt)
    }
    var res = Map[Int,Long](1->gs._1,0->gs._2.toLong)
    res
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
    var data = Utils.tsCols(df)
    var colName = Utils.tsCols(col)
    var none_data = data.filter(data(colName).isNull)
    data = data.filter(data(colName).isNotNull)
    data = data.select("label",colName).sort(colName)
    data.repartition(100)
    none_data.repartition(100)
    data.cache()
    none_data.cache()
    var count = getCount(data)
    var Seq(total_good,total_bad)=Seq(count.get(0).get,count.get(1).get)
    var res = List[Array[Double]]()
    //if (total_bad==0 || total_bad == 0 || total_good+total_bad<=10)
   //   return res
    count = getCount(none_data)

    var Seq(none_total_good,none_total_bad)=Seq(count.get(0).get,count.get(1).get)
    data = Utils.add_index(data)
    var parts = cut(data,10,colName)
    var  _ = getAllCount(data,parts)
    var Seq(acc_good,acc_bad)=Seq(0.toLong,0.toLong)

    var all_count = getAllCount(data,parts)
    for(index<-Range(1,parts.length)){
      val Seq(start,end)=Seq(parts(index-1),parts(index))
      var Seq(seq_good,seq_bad)=Seq(all_count.getOrElse((start,end,1),0).toString.toInt,all_count.getOrElse((start,end,1),0).toString.toInt)
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

    val path = "scala_test"//args(0)
    var df = Utils.tsCols(Utils.read(path))
    val features = df.columns.drop(5)
    features.map(line=>KS(df,line))


  }

}
