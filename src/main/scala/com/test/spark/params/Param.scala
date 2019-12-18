package com.test.spark.params

import scala.beans.BeanProperty

class Param {
    @BeanProperty var xgboost: XGBoostParam = null
    @BeanProperty var train: TrainParam = null
}
