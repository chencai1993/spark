package com.test.spark
import org.apache.spark.ml.feature.VectorAssembler
import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostModel}
import com.typesafe.config.ConfigFactory
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField}
import org.yaml.snakeyaml.Yaml
import java.io.{File, FileInputStream}

import com.test.spark.params.ReadParam

import scala.util.parsing.json
object Train {

  def main(args: Array[String]): Unit = {

    val param_handle = new ReadParam(args(0))
    val (train_params, xgb_params, missing, types) = param_handle.readHandle

    val spark = SparkEnv.getSession

    val train = Utils.tsCols(Utils.read(train_params.train_path,inferSchema = "true")).na.fill(missing)
    val test = Utils.tsCols(Utils.read(train_params.test_path,inferSchema = "true")).na.fill(missing)
    val fl = Utils.read(train_params.whitelist_path).select("feature_name").collect().map(line=>Utils.tsCols(line(0).toString))
    for(featurename<-fl){

    }
    val keys = train_params.key.split(" ")
    val vectorAssembler = new VectorAssembler().
      setInputCols(fl).
      setOutputCol("features")
    val xgb_input_train = vectorAssembler.transform(train).select("features", "label")
    val xgb_input_test = vectorAssembler.transform(test).select("features", "label")

    val xgbClassifier = new XGBoostEstimator(xgb_params).
      setFeaturesCol("features").
      setLabelCol("label")
      .setPredictionCol("score")
    val xgbModel = xgbClassifier.fit(xgb_input_train)
    println("模型训练完成")
    val test_data = xgb_input_test.rdd.map(r => new DenseVector(r.getAs[Vector]("features").toArray))
    val score = xgbModel.predict(test_data,missing)

    val test_key = test.select(keys.head,keys.tail:_*)
    val schema = test_key.schema.add(StructField("score", FloatType, nullable = true))
    val test_res = test_key.rdd.zip(score).map(line=>Row.fromSeq(line._1.toSeq ++ line._2.toSeq))
    val df = spark.createDataFrame(test_res,schema)
    Utils.write(df,train_params.test_res_path)

    val ks  = KS.KS(df.select("score","label").rdd.map(line=>(line(0),line(1))).collect())
    println(ks._1*100)


  }

}
