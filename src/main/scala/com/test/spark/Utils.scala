package com.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
object Utils {

  def tsCols(cols:Array[String],oldc:String="",newc:String=""): Array[String] ={
    var newcols:Array[String]=new Array[String](cols.length)
    for(i<-cols.indices){
      newcols(i)=cols(i).replace(oldc,newc)
    }
    newcols
  }
  def tsCols(df:DataFrame):DataFrame={
    df.toDF(tsCols(df.columns,".","#"):_*)
  }
  def tsCols(col:String):String={
    col.replace(".","#")
  }
  def rtsCols(df:DataFrame):DataFrame={
    df.toDF(tsCols(df.columns,"#","."):_*)
  }

  def read(path:String):DataFrame={
    val spark = SparkEnv.getSession
    return read(spark,path)
  }

  def read(spark:SparkSession,path:String,inferSchema:String="true"):DataFrame={
    val df = spark.read.option("delimiter","\t").option("header",true).option("inferSchema", "true").option("maxColumns",50000).csv(path=path)
    return df
  }
  def write(df:DataFrame,path:String,num_partition:Int = 200):Unit={
    df
      .write
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
  def getCols(df:DataFrame,col:String):Array[String]={
    df.select(col).collect().toArray.map(line=>line(0).toString)
  }
  def commonCols(leftDf: DataFrame, rightDf: DataFrame):List[String]={
    (leftDf.columns.toSet & rightDf.columns.toSet).toList
  }
  def commonColsNoName(leftDf: DataFrame, rightDf: DataFrame):List[String]={
    ((leftDf.columns.toSet & rightDf.columns.toSet) &~ Set("name","idcard","phone","loan_dt","label","uniq_id")).toList
  }
  def join(leftDf: DataFrame, rightDf: DataFrame,on: List[String]=List[String](),joinType:String): DataFrame = {
    val usingCols = if(on.nonEmpty) on else commonCols(leftDf, rightDf)
    val ts = udf((x:String)=>if(x==null)"None" else x)
    val rts = udf((x:String)=>if(x=="None")null else x)
    var Seq(leftDfts,rightDfts) =Seq(leftDf,rightDf)
    for(cols<-Array("idcard","phone")){
      leftDfts=leftDfts.withColumn(cols,ts(leftDf(cols)))
      rightDfts=rightDfts.withColumn(cols,ts(rightDfts(cols)))
    }
    var res = leftDfts.join(rightDfts,usingCols,joinType = joinType)
    for(cols<-Array("idcard","phone")){
      res=res.withColumn(cols,rts(res(cols)))
    }
    res
  }

  def add_index(df:DataFrame):DataFrame={
    val sub=udf((line:Long)=>line-1)
    var result = df.withColumn("index", row_number().over(Window.orderBy(monotonically_increasing_id())))
    result.withColumn("index",sub(result("index")))
  }



  def getPs(args: Array[String],prex:String="--"):Map[String,List[String]]={
    var params= Map[String,List[String]]()
    var key = ""
    for(item<-args){
      if(item.length>=prex.length && prex==item.substring(0,prex.length)){
        key = item.substring(prex.length)
        params+=(key->List[String]())
      }
      else{
        var value = params.get(key).get
        value = value:+item
        params+=(key->value)
      }
    }
    params
  }
  def getParam(params:Map[String,List[String]],key:String):String={
    var res = ""
    res = if(params.contains(key)) params.get(key).get(0) else ""
    res
  }



}
