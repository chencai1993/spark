package com.test.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkEnv{

  private val conf = new SparkConf()//.set("spark.executor.cores","2").setMaster("local")
  private val session = SparkSession.builder().appName("ferry-spark").config(this.conf).getOrCreate()

  private val sc: SparkContext = SparkEnv.getSession.sparkContext


  private val sqc: SQLContext = SparkEnv.getSession.sqlContext

  def getConf: SparkConf = this.conf

  def getSession: SparkSession = this.session

  def getSc: SparkContext = this.sc

  def getSqc: SQLContext = this.sqc
}

