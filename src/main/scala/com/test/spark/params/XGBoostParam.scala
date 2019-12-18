package com.test.spark.params

import scala.beans.BeanProperty

class XGBoostParam {
    @BeanProperty var eta: Double = 0.3
    @BeanProperty var max_depth: Int = 3
    @BeanProperty var gamma: Double = 0.0
    @BeanProperty var lambda: Double = 1.0
    @BeanProperty var alpha: Double = 0.0
    @BeanProperty var colsample_bylevel: Double = 1
    @BeanProperty var objective: String = "binary:logistic"
    @BeanProperty var min_child_weight: Int = 100
    @BeanProperty var booster: String = "gbtree"
    @BeanProperty var subsample: Double = 0.8
    @BeanProperty var nworkers: Int = 200
    @BeanProperty var eval_metric: String = "auc"
    @BeanProperty var num_round: Int = 1000
    @BeanProperty var numEarlyStoppingRounds: Int = 50
    @BeanProperty var use_external_memory: Boolean = true
    @BeanProperty var checkpointInterval: Int = 100
    @BeanProperty var checkpoint_path: String = ""
    @BeanProperty var checkpointInitialization: Boolean = false
    @BeanProperty var trainTestRatio: Double = 0.8
    @BeanProperty var scale_pos_weight: Double = 1.0
    @BeanProperty var colsample_bytree: Double = 1.0
    @BeanProperty var missing: Float = -999
    @BeanProperty var importance_type:String = "total_gain"
}
