package com.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.util.SizeEstimator
import util.control.Breaks._
object Utils {

  def rename(df:DataFrame,columns:Array[String]):DataFrame={
    val oldcolumns = df.columns
    if(oldcolumns.length !=columns)
       df
    var check = false
    for(i<-Range(0,oldcolumns.length)){
      if(oldcolumns(i)!=columns(i))
        {
          check=true
          break
        }
    }
    if(check)
      df.toDF(columns:_*)
    else
      df
  }
  def tsCols(cols:Array[String],oldc:String="",newc:String=""): Array[String] ={
    var newcols:Array[String]=new Array[String](cols.length)
    for(i<-cols.indices){
      newcols(i)=cols(i).replace(oldc,newc)
    }
    newcols
  }
  def tsCols(df:DataFrame):DataFrame={
    rename(df,tsCols(df.columns,".","#"))
  }
  def tsCols(col:String):String={
    col.replace(".","#")
  }
  def rtsCols(df:DataFrame):DataFrame={
    rename(df,tsCols(df.columns,"#","."))
  }
  def read(path:String):DataFrame={
    val spark = SparkEnv.getSession
    return read(spark,path)
  }
  def distict(df:DataFrame,cols:List[String]):DataFrame={
    distict(df,cols.toArray)
  }
  def distict(df:DataFrame,cols:Array[String]):DataFrame={
    val index = cols.map(line=>df.columns.indexOf(line))
    println("cols index")
    index.foreach(println)
    val res = df.rdd.map(line=>{
      var keys = List[String]()
      for(i<-index){
       if(line(i)==null)
         keys:+=""
        else
         keys:+=line(i).toString
      }
      val key = keys.mkString("#")
      (key,line)
    }).reduceByKey((x,y)=>x).map{case(key,value)=>value}
    val spark = SparkEnv.getSession
    spark.createDataFrame(res,df.schema)
  }


  def read(spark:SparkSession,path:String,inferSchema:String="true"):DataFrame={
    var df = spark.read.option("delimiter","\t").option("header",true).option("inferSchema", "false").option("maxColumns",50000).csv(path=path)
    println("rdd partions :"+df.rdd.partitions.length)
    if(df.columns.contains("loan_dt")){
      df=df.withColumn("loan_dt",Utils.formatLoan_dt(df("loan_dt")))
    }
    return df
  }
  def write(df:DataFrame,path:String):Unit={
    df
      .write
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }
  def write(df:DataFrame,path:String,num_partition:Int = 200):Unit={
    df
        .repartition(num_partition)
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
    println("------------------------usingCols--------------------")
    usingCols.foreach(println)
    println("------------------------usingCols--------------------")
    val ts = udf((x:String)=>if(x==null)"None" else x)
    val rts = udf((x:String)=>if(x=="None")null else x)
    var Seq(leftDfts,rightDfts) =Seq(leftDf,rightDf)
    for(cols<-(Set("idcard","phone") & usingCols.toSet)){
      leftDfts=leftDfts.withColumn(cols,ts(leftDf(cols)))
      rightDfts=rightDfts.withColumn(cols,ts(rightDfts(cols)))
    }
    var res = leftDfts.join(rightDfts,usingCols,joinType = joinType)
    for(cols<-(Set("idcard","phone") & usingCols.toSet)){
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

  val formatLoan_dt = udf((loan_dt:String) => loan_dt.substring(0, 10))

  def getTotalSize(rdd: RDD[Row]): Long = {
    // This can be a parameter
    val NO_OF_SAMPLE_ROWS = 10l;
    val totalRows = rdd.count();
    var totalSize = 0l
    if (totalRows > NO_OF_SAMPLE_ROWS) {
      val sampleRDD = rdd.sample(true, NO_OF_SAMPLE_ROWS)
      val sampleRDDSize = getRDDSize(sampleRDD)
      totalSize = sampleRDDSize.*(totalRows)./(NO_OF_SAMPLE_ROWS)
    } else {
      // As the RDD is smaller than sample rows count, we can just calculate the total RDD size
      totalSize = getRDDSize(rdd)
    }
    totalSize
  }
  def getRDDSize(rdd: RDD[Row]) : Long = {
    var rddSize = 0l
    val rows = rdd.collect()
    for (i <- 0 until rows.length) {
      rddSize += SizeEstimator.estimate(rows.apply(i).toSeq.map { value => value.asInstanceOf[AnyRef] })
    }
    rddSize
  }


  def main(args: Array[String]): Unit = {

  }


}
