package com.delta.svoc

trait customVariables {
  val customAppName = "Data Transformation_Raw-Published"
  val customSparkMaster = "local[*]"
  val rawPHXHiveDB = "svoc_phoenix"
  val stgPHXHiveDB = "svoc_stage_dev"
  //  val curPHXHiveDB = "svoc_curated_dev"
  //  val pubPHXHiveDB = "svoc_published_dev"
}
