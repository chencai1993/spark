package com.test.spark



import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import scala.collection.mutable.Set
import scala.util.Random
import java.io.PrintWriter
object KS {
  def cut(data:Array[(Double,Int)],Part:Int=10):Array[Double]={
    val count = data.length
    if(count<=Part)
      return Array[Double]()
    var l = Math.ceil(count*1.0/Part).toInt
    var pos = List[Int](0,l)
    for(i<-Range(2,Part+1))
    {
        pos = pos:+Math.min(i*l,count-1)
    }
    var parts = Set[Double]()
    for(i<-pos){
      parts.add(data(i)._1)
    }
    parts.toList.sorted.toArray
  }

  def isNum(v:Any):Boolean={
    try{
      v.toString.toDouble
      true
    }catch
      {
        case e:Exception => false
     }
  }
  def getKey(parts:Array[Double],value:Any,label:Int):(Double,Double,Int)={
    if(isNum(value)==false)
      return (0,-1,label)
    var v  = value.toString.toDouble
    for(i<-parts.indices.drop(1)){
      if(v<=parts(i))
        return (parts(i-1),parts(i),label)
    }
    (0,-1,label)
  }
  def getAllCount(data:Array[(Any,Any)],parts:Array[Double]):Map[(Double,Double,Int),Int]={
    var gs = Map[(Double,Double,Int),Int]()
    try {
      data.map(line => {
        val key = getKey(parts,line._1,line._2.toString.toInt)
        (key,1)
      }
      ).groupBy(_._1).mapValues(_.map(_._2).sum).toMap.foreach(line=>{gs+=(line._1->line._2)})
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
  def KS(data:Array[(Any,Any)]):List[Array[Double]]={
    var map = Map[String,Any]()
    var res = List[Array[Double]]()
    var filterdata = data.filter{case(v,l) => isNum(l)}
    var notnone_data = filterdata.filter{case(v,l) => isNum(v)}.map{case(v,l)=>(v.toString.toDouble,l.toString.toInt)}.sortBy(_._1)
    var parts = cut(notnone_data,10)
    if(parts.length==0)
      return res
    var parts_count = getAllCount(filterdata,parts)
    var Seq(none_total_good,none_total_bad)=Seq(0,0)
    var Seq(total_good,total_bad)=Seq(0,0)
    parts_count.foreach(line=>{
      if(line._1._3==0)
        if(line._1._1==0 && line._1._2== -1)
          none_total_good+=line._2;
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
      var Seq(seq_good,seq_bad)=Seq(parts_count.getOrElse((start,end,0),0).toString.toInt,parts_count.getOrElse((start,end,1),0).toString.toInt)
      acc_good+=seq_good
      acc_bad+=seq_bad
      val seq_res = getSeq(parts,index,seq_good,seq_bad,total_good,total_bad,none_total_good ,none_total_bad,acc_good,acc_bad)
      res:+=seq_res
    }

    res
  }
  def calculate_ks(df:DataFrame,out:String):Unit={
    var feature_to_index = Map[String,Int]()
    var index_to_feaature = Map[Int,String]()
    for(i<-df.columns.zipWithIndex){
      val feature = i._1
      val index = i._2
      feature_to_index += (feature -> index)
      index_to_feaature += (index -> feature)
    }
    var label_index = feature_to_index.get("label").get
    // columns to line
    var data = df.rdd.map(line=> {
      val label = line(label_index)
      line.toSeq.toArray.zipWithIndex.map{
        case(value,index)=>{
        (index,(value,label))
      }
      }
    }).flatMap(line=>line).groupByKey()
    val ks = data.mapValues{line=>{
      KS(line.toArray)
    }}
    val res = ks.collect()

    val outprint = new PrintWriter(out)
    res.foreach{
      case(index,value)=> {
        outprint.println(Utils.rtsCols(index_to_feaature.get(index).get))
        if (value.length > 0) {
          val head = List("seq", "开始", "结束", "订单数", "逾期数", "正常用户数", "百分比", "逾期率", "累计坏账户占比", "累计好账户占比", "KS", "IV").mkString("\t")
          outprint.println(head)
          for (line <- value) {
            outprint.println(line.mkString("\t"))
          }
        }
      }
    }
    outprint.close()
  }
  def filter_df(df:DataFrame,featurelist:Array[String]):DataFrame={
    val fl  = featurelist ++ Array[String]("label")
    df.select(fl.head,fl.tail:_*)
  }
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val out = args(1)
    var df = Utils.tsCols(Utils.read(path))
    df = filter_df(df,df.columns)
    calculate_ks(df,out)
  }

}
