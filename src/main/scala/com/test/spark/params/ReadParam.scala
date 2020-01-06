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
            "tree_method" -> params.xgboost.tree_method,
            "num_workers" -> params.xgboost.num_workers,
            "eval_metric" -> params.xgboost.eval_metric,
            "maximize_evaluation_metrics" -> params.xgboost.maximize_evaluation_metrics,
            "num_round" -> params.xgboost.num_round,
            "num_early_stopping_rounds" -> params.xgboost.num_early_stopping_rounds,
            "use_external_memory" -> params.xgboost.use_external_memory,
            "checkpoint_interval" -> params.xgboost.checkpoint_interval,
            "checkpoint_path" -> params.xgboost.checkpoint_path,
            "checkpoint_initialization" -> params.xgboost.checkpoint_initialization,
            "train_test_ratio" -> params.xgboost.train_test_ratio
        )
        (params.train, xgb_params_map, params.xgboost.missing, params.xgboost.importance_type)
    }
}
