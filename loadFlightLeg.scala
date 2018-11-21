package com.delta.svoc

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._

/*
 * Load flight leg data
 *
 * @author Suresh Narasimhan
 * Dated: 10-29-2018
 */

object loadFlightLeg extends customVariables {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().appName("PHX_Hist_Transformation").getOrCreate()
    val dffltleg = spark.sql("SELECT pcid,sfdc_case_id as fl_sfdc_case_id,flightnum,flightorigindate,scheduledoriginairportcode,scheduleddestinationairportcode FROM svoc_stage_dev.phx_feedback_temp")
    val df1 = dffltleg.groupBy("fl_sfdc_case_id").agg(collect_list(struct(col("flightNum"),col("flightOriginDate"),col("scheduledOriginAirportCode"),col("scheduledDestinationAirportCode"))).alias("compensationFlightLegs"))
    val df3 = df1.select("fl_sfdc_case_id",to_json("compensationFlightLegs").alias("compensationFlightLegs_str"))
    df3.createOrReplaceTempView("fltleg_temp_str")
    spark.sql(""" INSERT OVERWRITE TABLE svoc_stage_dev.phx_flt_temp_str select f.fl_sfdc_case_id,f.compensationFlightLegs_str from fltleg_temp_str f """)


  }

}
