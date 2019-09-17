package com.test.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession,SQLContext}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
object KS {
  def cut(data:DataFrame,Part:Int=10,col:String):Array[Double]={
    val count = data.count().toInt
    var l = Math.ceil(count*1.0/Part).toInt
    var pos = List[Int](0,1)
    for(i<-Range(2,Part+1))
    {
        pos = pos:+Math.min(i*l,count-1)
    }
    var parts = List[Double]()
    val values = data.filter(data.col("index").isin(pos:_*)).select(col).collect().toArray.map(line=>line(0).asInstanceOf[Double])
    values
  }

  def getSeq(parts:Array[Double],index:Int,seq_good:Long,seq_bad:Long,total_good:Long,total_bad:Long,none_total_good:Long,none_total_bad:Long): Array[Double]=
  {
    var res = List[Double]()
    /*
    res:+=index
    res:+=parts(index-1)
    res:+=parts(index)
    res:+=seq_bad+seq_good
    res:+=seq_bad
    res:+=seq_good
    res:+=(seq_good+seq_bad)*1.0/(total_good+total_bad)
    res:+=(seq_bad)*1.0/(seq_good+seq_bad)
   */
    res.toArray
  }
  def KS(df:DataFrame,col:String):Map[String,Any]={
    var map = Map[String,Any]()
    var data = Utils.tsCols(df)
    var colName = Utils.tsCols(col)
    var none_data = data.filter(data(colName).isNull)
    data = data.filter(data(colName).isNotNull).sort(colName)
    data = data.select("label",colName).sort(colName)
    var Seq(total_good,total_bad)=Seq(data.filter(data("label")===0).count(),data.filter(data("label")===1).count())
    var Seq(none_total_good,none_total_bad)=Seq(none_data.filter(data("label")===0).count(),none_data.filter(data("label")===1).count())
    data = Utils.add_index(data)
    var parts = cut(data,10,colName)
    for(index<-Range(1,parts.length)){
      if(index==1){
        var t = data.filter(data(col)>=parts(index-1) && data(col)<parts(index))
        val seq_good = t.filter(t("label")===0).count()
        val seq_bad = t.filter(t("label")===1).count()
        val seq_res = getSeq(parts,index,seq_good,seq_bad,total_good,total_bad,none_total_good ,none_total_bad)
      }
    }
    map
  }

  import org.apache.spark.sql.functions.udf

  def main(args: Array[String]): Unit = {

    val path = "scala_test"
    var df = Utils.read(path)
    val col = "shaohua.inop.all.v01.score"
    KS(df,col)
  }



}
