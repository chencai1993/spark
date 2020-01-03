package com.test.spark
import org.apache.spark.ml.feature.VectorAssembler
import com.typesafe.config.ConfigFactory
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StructField}
import org.yaml.snakeyaml.Yaml
import java.io.{File, FileInputStream}
import org.apache.spark.sql.functions.udf
import Array._
import com.test.spark.params.ReadParam
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.sql.functions.col

import scala.util.parsing.json
object Train {

  def String2Double(df:DataFrame,fl:Array[String]):DataFrame={
    var needts = false
    val tscols = df.dtypes.map(s=>{
      if(s._1 == "label")
        col(s._1).cast(IntegerType)
      else if ((s._2 == "StringType") && (fl.contains(s._1))){
        needts = true
        col(s._1).cast(DoubleType)
      }
      else
        col(s._1)
    })
    if(needts)
      df.select(tscols: _*)
    else
      df
  }
  def main(args: Array[String]): Unit = {

    val param_handle = new ReadParam("params.yaml")
    val (train_params, xgb_params, missing, types) = param_handle.readHandle
    val spark = SparkEnv.getSession
    var train = Utils.tsCols(Utils.read(train_params.train_path,inferSchema = "true")).repartition(xgb_params.get("nworkers").get.toString.toInt)
    var test = Utils.tsCols(Utils.read(train_params.test_path,inferSchema = "true")).repartition(xgb_params.get("nworkers").get.toString.toInt)
    val fl = Utils.read(train_params.whitelist_path).select("feature_name").collect().map(line=>Utils.tsCols(line(0).toString))
    train = String2Double(train,fl)
    test = String2Double(test,fl)
    train = train.na.fill(missing)
    test = test.na.fill(missing)
    val keys = train_params.key.split(" ")
    val vectorAssembler = new VectorAssembler().
      setInputCols(fl).
      setOutputCol("features")
    var out_cols= Array("features") ++ keys
    val xgb_input_train = vectorAssembler.transform(train).select(out_cols.head,out_cols.tail:_*)
    val xgb_input_test = vectorAssembler.transform(test).select(out_cols.head,out_cols.tail:_*)
    val xgbClassifier = new XGBoostClassifier(xgb_params).
      setFeaturesCol("features").
      setLabelCol("label")
      .setPredictionCol("score")
      .setEvalSets(Map("test"->xgb_input_test))
    val xgbModel = xgbClassifier.fit(xgb_input_train)
    println("模型训练完成")
    xgbModel.nativeBooster.saveModel(train_params.local_model_path)
    val test_data = xgbModel.transform(xgb_input_test)
    val get_score = udf((v:DenseVector)=>v(1))
    out_cols=keys ++ Array("probability")
    val score = test_data.select(out_cols.head,out_cols.tail:_*).withColumn("probability",get_score(col("probability")))
    Utils.write(score,train_params.test_res_path)
    val ks  = KS.KS(score.select("probability","label").rdd.map(line=>(line(0),line(1))).collect())
    println(ks._1*100)

  }

}
