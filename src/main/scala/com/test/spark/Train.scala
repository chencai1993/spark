package com.test.spark
import org.apache.spark.ml.feature.VectorAssembler
import ml.dmlc.xgboost4j.scala.spark.{XGBoostEstimator, XGBoostModel}
object Train {

  def main(args: Array[String]): Unit = {

    val train = Utils.tsCols(Utils.read("train",inferSchema = "true")).na.fill(-999)
    val test = Utils.tsCols(Utils.read("test",inferSchema = "true")).na.fill(-999)
    val fl = Utils.read("featurelist").select("feature_name").collect().map(line=>Utils.tsCols(line(0).toString))
    val vectorAssembler = new VectorAssembler().
      setInputCols(fl).
      setOutputCol("features")

    val xgb_input_train = vectorAssembler.transform(train).select("features", "label")
    val xgbParam = Map("eta" -> 0.1f,
      "missing" -> -999,
      "num_class" -> 2,
      "num_round" -> 100,
      "num_workers" -> 1,
      "num_early_stopping_rounds" -> 10
    )
    val xgbClassifier = new XGBoostEstimator(xgbParam).
      setFeaturesCol("features").
      setLabelCol("label")
      .setPredictionCol("score")
    val xgbModel = xgbClassifier.fit(xgb_input_train)
    val test_res = xgbModel.transform(xgb_input_train).select("score","label").collect().map(line=>(line(0),line(1)))
    val ks  = KS.KS(test_res)
    println(ks)
    val t = ks

  }

}
