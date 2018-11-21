package com.delta.svoc

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._

import scala.io.Source._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path



trait phxsparkContextCustom extends rtcustomvariables {

  val conf: SparkConf = new SparkConf().setAppName(customAppName).setMaster(customSparkMaster)
  val spark: SparkSession = SparkSession.builder()
    .appName(customAppName)
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()

  var hadoopConf = new org.apache.hadoop.conf.Configuration()

}

object PHXRTLoad extends phxsparkContextCustom {

  def main(args: Array[String]): Unit = {

    var Path :String = args(0)

    val phxDF = spark.read.option("multiline","true").json(Path)
//    phxDF.show()

    val df1 = phxDF.select("customerCaseSummary.*")

    println("************************CHECK if compensation exists in JSON *********************** ")


    val df2 = df1.selectExpr("customerCase.customerCaseCompensation")

    df2.createOrReplaceTempView("comp_test_view")

    val df3 = spark.sql("select comp.compensationCaseId from comp_test_view lateral view explode(customerCaseCompensation) a as comp limit 1")

    println("************************CHECK if compensation exists in JSON completed *********************** ")

    //Proceed with JSON processing if Compensation data exists
    if (df3.limit(1).take(1).length > 0)
{

        println("===============================================================")
        println("Calling parsePHXJSON- to parse JSON ")
        println("===============================================================")

        parsePHXJson.parseJSON(df1)

        println("===============================================================")
        println("Calling parsePHXJSON-build custcasecompensation ")
        println("===============================================================")
//        parsePHXJson.buildcustcasecompensation()

        println("===============================================================")
        println("Calling parsePHXJSON-loadCurated ")
        println("===============================================================")
        //        parsePHXJson.loadCurated()

        println("===============================================================")
        println("Calling parsePHXJSON-loadPublished ")
        println("===============================================================")
        //        parsePHXJson.loadPublished()

}


    spark.stop()

  }


}
