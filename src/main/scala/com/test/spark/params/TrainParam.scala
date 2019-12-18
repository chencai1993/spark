package com.test.spark.params

import scala.beans.BeanProperty

class TrainParam {
    @BeanProperty var train_path: String = ""
    @BeanProperty var test_path: String = ""
    @BeanProperty var whitelist_path: String = ""
    @BeanProperty var hdfs_model_path: String = ""
    @BeanProperty var local_model_path: String = ""
    @BeanProperty var local_model_feature_path: String = ""
    @BeanProperty var local_model_feature_weights_path: String = ""
    @BeanProperty var local_quantiles_path: String = ""
    @BeanProperty var train_res_path: String = ""
    @BeanProperty var test_res_path: String = ""
    @BeanProperty var cv_num: Int = 5
    @BeanProperty var key: String = ""
}
