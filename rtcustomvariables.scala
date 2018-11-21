package com.delta.svoc

trait rtcustomvariables {
  val customAppName = "Real TIme Data Transformation_Raw-Published"
  val customSparkMaster = "local[*]"
  val jsonPath = "/data/dev/svocmonitor/raw/phoenix/"
  val rawPHXHiveDB = "svoc_phoenix"
  val stgPHXHiveDB = "svoc_stage_dev"
  //  val curPHXHiveDB = "svoc_curated_dev"
    val pubPHXHiveDB = "svoc_published_dev"

}
