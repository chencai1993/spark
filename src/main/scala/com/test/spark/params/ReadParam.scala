package com.test.spark.params

import java.io.{FileInputStream, FileNotFoundException, InputStream}

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

class ReadParam(param_path: String) {
    def readHandle: (TrainParam, Map[String, Any], Float, String) = {
        var input: InputStream = null
        try {
            input = new FileInputStream(param_path)
        } catch {
            case e: FileNotFoundException => throw new RuntimeException(e)
        }
        val yaml = new Yaml(new Constructor(classOf[Param]))
        val params = yaml.load(input).asInstanceOf[Param]
        val train_params = params.train
        val train_path = train_params.train_path
        val test_path = train_params.test_path
        println("****************train/test path**********************")
        println(train_path)
        println(test_path)
        println("****************train/test path**********************")
        //xgboost超参数
        //Exception in thread "main" java.lang.IllegalStateException: Unable to get 200 workers for XGBoost training
        val xgb_params_map = Map(
            "eta" -> params.xgboost.eta,
            "max_depth" -> params.xgboost.max_depth,
            "gamma" -> params.xgboost.gamma,
            "lambda" -> params.xgboost.lambda,
            "alpha" -> params.xgboost.alpha,
            "colsample_bylevel" -> params.xgboost.colsample_bylevel,
            "scale_pos_weight" -> params.xgboost.scale_pos_weight,
            "colsample_bytree" -> params.xgboost.colsample_bytree,
            "missing" -> params.xgboost.missing,
            "objective" -> params.xgboost.objective,
            "min_child_weight" -> params.xgboost.min_child_weight,
            "booster" -> params.xgboost.booster,
            "subsample" -> params.xgboost.subsample,
            "nworkers" -> params.xgboost.nworkers,
            "eval_metric" -> params.xgboost.eval_metric,
            "num_round" -> params.xgboost.num_round,
            "numEarlyStoppingRounds" -> params.xgboost.numEarlyStoppingRounds,
            "use_external_memory" -> params.xgboost.use_external_memory,
            "checkpointInterval" -> params.xgboost.checkpointInterval,
            "checkpoint_path" -> params.xgboost.checkpoint_path,
            "checkpointInitialization" -> params.xgboost.checkpointInitialization,
            "trainTestRatio" -> params.xgboost.trainTestRatio
        )
        (params.train, xgb_params_map, params.xgboost.missing, params.xgboost.importance_type)
    }
}
