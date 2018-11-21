package com.delta.svoc


import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.to_json
import scala.io.Source._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


/*
 * Read HQLs and data path queries from a file and load it to staging area
 *
 * @author Suresh Narasimhan
 * Dated: 10-29-2018
 */


trait sparkContextCustom extends customVariables {

  val conf: SparkConf = new SparkConf().setAppName(customAppName).setMaster(customSparkMaster)
  val spark: SparkSession = SparkSession
    .builder()
    .appName(customAppName)
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()

  var hadoopConf = new org.apache.hadoop.conf.Configuration()
  var fileSystem = FileSystem.get(hadoopConf)


}


object LoadPHXHistData extends sparkContextCustom {

  def main(args: Array[String]): Unit = {

    var Path = new Path(args(0))
    val inputStream = fileSystem.open(Path)
    var Properties = new java.util.Properties
    Properties.load(inputStream)

    val spark = SparkSession.builder().enableHiveSupport().appName("Hist Transformation Load").getOrCreate()

//    val dfcomp = spark.sql(Properties.getProperty("query1"))
//     dfcomp.printSchema()

//     dfcomp.show(5)
//     dfcomp.write.format("orc").mode("overwrite").save(Properties.getProperty("path1"))
//    dfcomp.write.format("orc").mode("overwrite").save("/data/svoc/dev/stage/phx_compensation_temp")


    val dffdbk = spark.sql(Properties.getProperty("query2"))
    dffdbk.printSchema()
    dffdbk.write.format("orc").mode("overwrite").save("/data/svoc/dev/stage/phx_feedback_temp")

//    val dfpnr = spar  k.sql(Properties.getProperty("query3"))
//    dfpnr.printSchema()

//    val dftkt = spark.sql(Properties.getProperty("query4"))
//    dftkt.printSchema()

//    val dfrpad = spark.sql(Properties.getProperty("query5"))
//    dfrpad.printSchema()

//    dffdbk.write.format("orc").mode("overwrite").save("/data/svoc/dev/curated/phx_comp_fdbk_temp")

    spark.stop()
  }

}
