package com.test.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkEnv{

  //System.setProperty("hadoop.home.dir", "D:/")
  //private val l_conf = new SparkConf().setMaster("local").setAppName("tianji-spark")
  private val c_conf = new SparkConf()
  private val conf = c_conf
  private val session = SparkSession.builder().appName("tianji-spark").config(this.conf).getOrCreate()

  private val sc: SparkContext = SparkEnv.getSession.sparkContext

  private val sqc: SQLContext = SparkEnv.getSession.sqlContext

  def getConf: SparkConf = this.conf

  def getSession: SparkSession = this.session

  def getSc: SparkContext = this.sc

  def getSqc: SQLContext = this.sqc
}

