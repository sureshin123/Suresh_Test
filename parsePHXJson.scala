package com.delta.svoc

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions._
import scala.util.{Try, Success, Failure}
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


trait sparkContextCustom extends rtcustomvariables {

  val conf: SparkConf = new SparkConf().setAppName(customAppName).setMaster(customSparkMaster)
  val spark: SparkSession = SparkSession.builder()
    .appName(customAppName)
    .config("spark.driver.allowMultipleContexts", "true")
    .enableHiveSupport()
    .getOrCreate()

  var hadoopConf = new org.apache.hadoop.conf.Configuration()

}

object parsePHXJson extends sparkContextCustom{


  def parseJSON (df1 : DataFrame) : Unit = {

    println("===============================================================")
    println("Parsing PHX Real-Time JSON Data")
    println("===============================================================")

    import spark.implicits._

    val custdf1 = df1.selectExpr("customer.persistentCustomerId as pcid"
      , "customer.customerAgent.recordId as sfdc_acct_id"
      , "customer.customerName.firstName as frst_nm"
      , "customer.customerName.lastName as lst_nm"
      , "customer.customerName.middleName as mid_nm"
      , "customer.customerName.nameSuffixCode as acct_sfx_cd"
      , "customer.birthDate as brth_dt"
      , "customer.primaryEmailAdr as acct_email_adr"
      , "customer.workEmailAdr as wrk_email_adr"
      , "customer.otherEmailAdr as othr_email_adr"
      , "customer.mailingAdr.addressLine1Text as mlng_str_adr"
      , "customer.mailingAdr.cityLocalityName as mlng_cty_nm"
      , "customer.mailingAdr.districtTownName as mlng_st_cd"
      , "customer.mailingAdr.countryCode as mlng_ctry_txt"
      , "customer.mailingAdr.postalCode as mlng_pstl_cd"
      , "customer.mobileNum as mobl_ph_nb"
      , "customer.otherAdr.addressLine1Text as othr_str_adr"
      , "customer.otherAdr.cityLocalityName as othr_cty_nm"
      , "customer.otherAdr.districtTownName as oth_st_cd"
      , "customer.otherAdr.countryCode as othr_ctry_txt"
      , "customer.otherAdr.postalCode as othr_pstl_cd"
      , "customer.otherPhoneNum as othr_ph_nb"
      , "customer.primaryPhoneNum as ph_nb"
      , "customer.contactedDot as ctcd_dot_ind"
      , "customer.excessiveCompensation as excsv_comp_ind"
      , "customer.fraud as frd_ind"
      , "customer.noFly as no_fly_ind"
      , "customer.threatenedDot as dot_thrt_ind"
      , "customer.warningLetter as wrng_lttr_ind"
      , "customer.loyaltyMembers"
    )

    custdf1.createOrReplaceTempView("cust_temp1")
    custdf1.printSchema()
    val loyaltydf = custdf1.select($"pcid",explode_outer($"loyaltyMembers").as("loyalty"))
        loyaltydf.show()
    if(custdf1.select(explode_outer($"loyaltyMembers").as("lo")).where("lo is null").count < 1)
      {
        val custdf2 = custdf1.alias("c").join(loyaltydf.alias("l"),$"c.pcid" === $"l.pcid","left_outer").selectExpr("c.pcid"
          ,"c.sfdc_acct_id"
          ,"c.frst_nm"
          ,"c.lst_nm"
          ,"c.mid_nm"
          ,"c.acct_sfx_cd"
          ,"c.brth_dt"
          ,"c.acct_email_adr"
          ,"c.wrk_email_adr"
          ,"c.othr_email_adr"
          ,"c.mlng_str_adr"
          ,"c.mlng_cty_nm"
          ,"c.mlng_st_cd"
          ,"c.mlng_ctry_txt"
          ,"c.mlng_pstl_cd"
          ,"c.mobl_ph_nb"
          ,"c.othr_str_adr"
          ,"c.othr_cty_nm"
          ,"c.oth_st_cd"
          ,"c.othr_ctry_txt"
          ,"c.othr_pstl_cd"
          ,"c.othr_ph_nb"
          ,"c.ph_nb"
          ,"c.ctcd_dot_ind"
          ,"c.excsv_comp_ind"
          ,"c.frd_ind"
          ,"c.no_fly_ind"
          ,"c.dot_thrt_ind"
          ,"c.wrng_lttr_ind"
          ,"l.loyaltyMemberId"
          ,"l.carrierCode"
        )
//        custdf2.printSchema()
        custdf2.createOrReplaceTempView("cust_temp")
//        println ("PRINTING CUSTDF2.....................")
//        custdf2.show()
      } else{
      val custdf3 = custdf1.alias("c").join(loyaltydf.alias("l"),$"c.pcid" === $"l.pcid","left_outer").selectExpr("c.pcid"
        ,"c.sfdc_acct_id"
        ,"c.frst_nm"
        ,"c.lst_nm"
        ,"c.mid_nm"
        ,"c.acct_sfx_cd"
        ,"c.brth_dt"
        ,"c.acct_email_adr"
        ,"c.wrk_email_adr"
        ,"c.othr_email_adr"
        ,"c.mlng_str_adr"
        ,"c.mlng_cty_nm"
        ,"c.mlng_st_cd"
        ,"c.mlng_ctry_txt"
        ,"c.mlng_pstl_cd"
        ,"c.mobl_ph_nb"
        ,"c.othr_str_adr"
        ,"c.othr_cty_nm"
        ,"c.oth_st_cd"
        ,"c.othr_ctry_txt"
        ,"c.othr_pstl_cd"
        ,"c.othr_ph_nb"
        ,"c.ph_nb"
        ,"c.ctcd_dot_ind"
        ,"c.excsv_comp_ind"
        ,"c.frd_ind"
        ,"c.no_fly_ind"
        ,"c.dot_thrt_ind"
        ,"c.wrng_lttr_ind"
        ,"'' as loyaltyMemberId"
        ,"'' as carrierCode"
      )
//      println ("PRINTING CUSTDF3.....................")
//      custdf3.printSchema()
      custdf3.createOrReplaceTempView("cust_temp")
//      custdf3.show()
    }

     val custdf = spark.sql("SELECT * FROM cust_temp")
//    println ("PRINTING CUSTDF.....................")
//    custdf.printSchema()


//    custdf.show()

    println("===============================================================")
    println("Parsing PHX Real-Time JSON to read Case Data")
    println("===============================================================")


    val casedf = df1.selectExpr("customer.persistentCustomerId as pcid"
      , "customer.customerAgent.recordId as sfdc_acct_id"
      , "customerCase.customerCaseAgent.recordId as case_sfdc_case_id"
      ,"customerCase.customerCaseAgent.createdbyUserId as case_sfdc_usr_id"
      , "customerCase.caseNum as case_nb"
      , "customerCase.caseOriginDesc as case_orig_src_nm"
      , "customerCase.customerCaseRecordTypeCode as case_rec_typ_id"
      , "customerCase.caseClosedUtcTs as case_clsd_ts"
      , "customerCase.caseCreatedDate as case_crtd_ts"
      , "customerCase.caseComplaintDesc as case_orgl_ds_txt"
      , "customerCase.caseStatusCode as case_stt_nm"
      , "customerCase.subjectDesc as case_srt_ds_txt"
      , "customerCase.caseTypeDesc as case_typ_nm"
      , "customerCase.accountNum"
      , "customerCase.baggageDOTDisabilityCode as dot_dsbly_rsn_txt"
      , "customerCase.caseSummaryText as case_shrt_ds_txt"
      , "customerCase.baggageClaimAmt as clm_amt"
      , "customerCase.deniedBagClaimReasonText as dnd_bag_clm_rsn_ds"
      , "customerCase.baggageDisabilityItemText"
      , "customerCase.psrExcludedReasonText as excld_rsn_txt"
      , "customerCase.baggageExclusionAmt as excn_amt"
      , "customerCase.baggageExclusionReasonText as excn_rsn_txt"
      , "customerCase.inboundLanguageCode as ib_lang_cd"
      , "customerCase.initialReceivedDate as init_case_rcvd_dt"
    )

    casedf.createOrReplaceTempView("case_temp")

//    casedf.show()


    println("===============================================================")
    println("Parsing PHX Real-Time JSON to read Relationship Data")
    println("===============================================================")

    val dfctac = df1.selectExpr("customerCase.customerCaseRelationship")
//    dfctac.printSchema()

    if(dfctac.select(explode_outer($"customerCaseRelationship").as("rlshp")).where("rlshp is null").count < 1)
    {
      dfctac.createOrReplaceTempView("ctac_temp")

      dfctac.show()
    }
    println("===============================================================")
    println("Parsing PHX Real-Time JSON to read Compensation Data")
    println("===============================================================")


    val cmpdf = df1.selectExpr("customerCase.customerCaseAgent.recordId as case_sfdc_case_id"
      ,"customerCase.caseNum as case_nb"
      ,"customerCase.customerCaseCompensation"
    )

    cmpdf.createOrReplaceTempView("comp_temp1")


    val comptempdf1 = spark.sql("select case_sfdc_case_id,case_nb,comp.customerCaseAgent.recordId as sfdc_comp_typ_id,comp.customerCaseAgent.createdbyUserId as comp_typ_crtd_by_usr_id,comp.customerCaseAgent.recordLastUpdatedUtcTs as comp_crtd_ts,comp.customerCaseAgent.recordLastUpdatedUtcTs as comp_typ_lst_mdfd_ts,comp.compensationCustomerId as comp_typ_acct_nb,comp.compensationCaseId as comp_typ_assd_sfdc_case_id,comp.compensationUuidNum as uuid_txt,comp.compensationRecordTypeCode as comp_typ_ds,comp.compensationReasonText as comp_rsn_txt,comp.compensationValue.compensationAmt.currencyAmt as comp_amt_arr,comp.compensationValue.compensationAmt.currencyCode as curr_cd_txt_arr,comp.compensationValue.compensationMilesCnt as sm_val_nb_arr,comp.giftBasketMessageText as Gift_Bskt_Val_Txt,comp.refundReasonText as rfnd_rsn_txt,comp.documentNum as tkdc_nb,comp.issuingBankName as bnk_nm,comp.bankAdr as bnk_adr,comp.bankPhoneNum as bnk_ph_nb,comp.bankCode as bnk_brch_nb,comp.branchName,comp.branchCode,comp.customerName as cust_nm,comp.statusCode as comp_stt_cd from comp_temp1 lateral view explode(customerCaseCompensation) a as comp").distinct().toDF()


    comptempdf1.createOrReplaceTempView("comp_temp2")
    val compdf = spark.sql("select case_sfdc_case_id,case_nb,sfdc_comp_typ_id,comp_typ_crtd_by_usr_id,comp_crtd_ts,comp_typ_lst_mdfd_ts," +
      "comp_typ_acct_nb,comp_typ_assd_sfdc_case_id,uuid_txt,comp_typ_ds,comp_rsn_txt,comp_amt,curr_cd_txt ,sm_val_nb,Gift_Bskt_Val_Txt," +
      "rfnd_rsn_txt,tkdc_nb,bnk_nm,bnk_adr,bnk_ph_nb,bnk_brch_nb,branchName,branchCode,cust_nm,comp_stt_cd " +
      "from comp_temp2 lateral view explode(comp_amt_arr) as comp_amt  lateral view explode(curr_cd_txt_arr) as curr_cd_txt lateral view explode (sm_val_nb_arr) as sm_val_nb").distinct().toDF()

    compdf.createOrReplaceTempView("comp_temp")
//    compdf.show()


    println("===============================================================")
    println("Parsing PHX Real-Time JSON to read Pnr Data")
    println("===============================================================")


    val pnrdf1 = df1.selectExpr("customerCase.customerCaseAgent.recordId as case_sfdc_case_id"
      ,"customerCase.caseNum as case_nb"
      ,"customerCase.booking"
    )

    pnrdf1.createOrReplaceTempView("pnr_temp1")


    val pnrtempdf1 = spark.sql("select case_sfdc_case_id,case_nb,pnr.customerCaseAgent.recordId as sfdc_pnr_id" +
      ",pnr.customerCaseAgent.createdbyUserId as pnr_crtd_by_usr_id,pnr.customerCaseAgent.recordLastUpdatedUtcTs as PNR_Lst_Mdfd_Ts " +
      ",pnr.bookingCaseId as PNR_SFDC_Case_Id,pnr.recordLocatorId as PNR_Rec_Locr_Id" +
      " from pnr_temp1 lateral view explode(booking) a as pnr").distinct().toDF()

//    pnrtempdf1.printSchema()
//    pnrtempdf1.show()

    pnrtempdf1.createOrReplaceTempView("pnr_temp")

    println("===============================================================")
    println("Parsing PHX Real-Time JSON to read Tkt Data")
    println("===============================================================")


    val tktdf1 = df1.selectExpr("customerCase.customerCaseAgent.recordId as case_sfdc_case_id"
      ,"customerCase.caseNum as case_nb"
      ,"customerCase.accountableDocument"
    )

    tktdf1.createOrReplaceTempView("tkt_temp1")

    val tkttempdf1 = spark.sql("select case_sfdc_case_id,case_nb,tkt.customerCaseAgent.recordId as sfdc_tkt_id" +
      ",tkt.customerCaseAgent.createdbyUserId as TKT_Crtd_By_Usr_Id,tkt.customerCaseAgent.recordLastUpdatedUtcTs as TKT_Lst_Mdfd_Ts" +
      ",tkt.accountableDocumentCaseId as TKT_SFDC_Case_Id,tkt.accountableDocumentNum as Tkt_Doc_Nb,tkt.ticketDesignatorCode as Tkt_Dsgtr_Cd,tkt.seatId as seat_id " +
      " from tkt_temp1 lateral view explode(accountableDocument) a as tkt").distinct().toDF()

//    tkttempdf1.printSchema()
//    tkttempdf1.show()

    tkttempdf1.createOrReplaceTempView("tkt_temp")


//    println("===============================================================")
//    println("Parsing PHX Real-Time JSON to read Case-CustomerFlightLeg Data")
//    println("===============================================================")
//

    val dffltleg_temp1 = df1.selectExpr("customerCase.customerCaseFlightLeg.caseFlightLegCaseId as case_sfdc_case_id"
      ,"customerCase.customerCaseFlightLeg"
    )

    dffltleg_temp1.printSchema()
//    dffltleg_temp1.show()


//    if(dffltleg_temp1.select(explode_outer($"customerCaseFlightLeg").as("flt")).where("flt is null").count() == 0 )
//      {

        println("HAVING Flight Leg INFO *****************************")
        dffltleg_temp1.createOrReplaceTempView("fltleg_temp1")

        val dffleg = spark.sql("select case_sfdc_case_id,flt.customerCaseAgent.recordId as sfdc_flt_leg_id," +
          "flt.customerCaseAgent.createdbyUserId as flt_crtd_by_usr_id,flt.customerCaseAgent.recordLastUpdatedUtcTs as flt_crtd_ts, " +
          "flt.caseFlightLegCaseId as flt_sfdc_case_id,flt.costPerSegmentAmt,flt.mileageCnt ,flt.mileagePercentageNum, " +
          "flt.flightleg from fltleg_temp1 lateral view explode(customerCaseFlightLeg) a as flt").distinct().toDF()

        dffleg.printSchema()
        dffleg.show()

        dffleg.createOrReplaceTempView("fltleg_temp2")

//      }



    //    println("===============================================================")
    //    println("Parsing PHX Real-Time JSON to read Case-Feedback Data")
    //    println("===============================================================")
    //

    val dffdbk1 = df1.selectExpr("customerCase.customerCaseAgent.recordId as case_sfdc_case_id"
      ,"customerCase.caseNum as case_nb"
      ,"customerCase.customerCaseComplaint"
    )

    if(dffdbk1.select(explode_outer($"customerCaseComplaint").as("compl")).where("compl is null").count() == 0 )
    {
      dffdbk1.createOrReplaceTempView("fdbk_temp1")


      val dffdbk2 = spark.sql("select case_sfdc_case_id,case_nb,fdbk.customerCaseAgent.recordId as sfdc_case_fdbk_id," +
        "fdbk.customerCaseAgent.createdbyUserId as fdbk_crtd_by_usr_id,fdbk.customerCaseAgent.recordLastUpdatedUtcTs as fdbk_crtd_ts," +
        "fdbk.complaintRecordTypeCode as rec_typ_id,fdbk.bookingClassOfServiceCode,fdbk.cabinNum as cabn_cls_cd,fdbk.cabinClassOperatingCarrierCode as oprg_crr_cd" +
        "fdbk.complaintCityCode as case_fdbk_cty_nm,fdbk.complaintCodes as complaintCodes_arr,fdbk.complaintLocationText as case_fdbk_loc_nm," +
        "fdbk.complaintSsrText as ssr_psnt_ind,fdbk.flightleg" +
        "from fdbk_temp1 lateral view explode(customerCaseComplaint) a as fdbk").distinct().toDF()

      dffdbk2.createOrReplaceTempView("fdbk_temp2")

      val dffdbk3 = spark.sql("select case_sfdc_case_id,sfdc_case_fdbk_id," +
        "fdbk_crtd_by_usr_id,fdbk_crtd_ts," +
        "rec_typ_id,bookingClassOfServiceCode,cabn_cls_cd,oprg_crr_cd" +
        "case_fdbk_cty_nm,complaintCodes,case_fdbk_loc_nm,ssr_psnt_ind,flightleg" +
        "from fdbk_temp1 lateral view explode(complaintCodes_arr) a as complainCodes").distinct().toDF()

      dffdbk3.createOrReplaceTempView("fdbk_temp3")

      val dffdbk4 = spark.sql("select case_sfdc_case_id,sfdc_case_fdbk_id," +
        "fdbk_crtd_by_usr_id,fdbk_crtd_ts," +
        "rec_typ_id,bookingClassOfServiceCode,cabn_cls_cd,oprg_crr_cd" +
        "case_fdbk_cty_nm,complaintCodes,case_fdbk_loc_nm,fdbk.complaintSsrText as ssr_psnt_ind,ssr_psnt_ind,flt.customerCaseAgent.recordId as flt_leg_id,flt.flightleg" +
        "from fdbk_temp1 lateral view explode(flightleg) a as flt").distinct().toDF()

      dffdbk4.printSchema()
      dffdbk4.show()

      dffdbk4.createOrReplaceTempView("fdbk_temp")
    }


    // Join Case and Compensation
    println("===============================================================")
    println("JOINING Case Compensation ")
    println("===============================================================")
    val case_comp_df = casedf.alias("cas").join(compdf.alias("comp"),$"cas.case_sfdc_case_id" === $"comp.case_sfdc_case_id" && $"cas.case_nb" === $"comp.case_nb","left_outer").selectExpr(
      "pcid"
      ,"sfdc_acct_id"
      ,"cas.case_sfdc_case_id"
      ,"cas.case_nb"
      ,"case_orig_src_nm"
      ,"case_rec_typ_id"
      ,"case_clsd_ts"
      ,"case_crtd_ts"
      ,"case_orgl_ds_txt"
      ,"case_stt_nm"
      ,"case_srt_ds_txt"
      ,"case_typ_nm"
      ,"case_sfdc_usr_id"
      ,"dot_dsbly_rsn_txt"
      ,"case_shrt_ds_txt"
      ,"clm_amt"
      ,"dnd_bag_clm_rsn_ds"
      ,"baggageDisabilityItemText"
      ,"excld_rsn_txt"
      ,"excn_amt"
      ,"excn_rsn_txt"
      ,"ib_lang_cd"
      ,"init_case_rcvd_dt"
      ,"sfdc_comp_typ_id"
      ,"comp_typ_crtd_by_usr_id"
      ,"comp_crtd_ts"
      ,"comp_typ_lst_mdfd_ts"
      ,"comp_typ_acct_nb"
      ,"comp_typ_assd_sfdc_case_id"
      ,"uuid_txt"
      ,"comp_typ_ds"
      ,"comp_rsn_txt"
      ,"comp_amt"
      ,"curr_cd_txt"
      ,"sm_val_nb"
      ,"Gift_Bskt_Val_Txt"
      ,"rfnd_rsn_txt"
      ,"tkdc_nb"
      ,"bnk_nm"
      ,"bnk_adr"
      ,"bnk_ph_nb"
      ,"bnk_brch_nb"
      ,"branchName"
      ,"branchCode"
      ,"cust_nm"
      ,"comp_stt_cd"
    ).distinct()
    case_comp_df.cache()

//    println("Printing Case:Compensation................................ ")
//    case_comp_df.show()

    // Join CaseComp with Customer
    println("===============================================================")
    println("JOINING CaseCompensation with Customer ")
    println("===============================================================")
    val cust_case_comp_df = case_comp_df.alias("cc").join(custdf.alias("cust"),$"cust.sfdc_acct_id" === $"cc.comp_typ_acct_nb","left_outer").selectExpr(
      "cc.pcid"
      ,"cust.sfdc_acct_id"
      ,"cust.frst_nm"
      ,"cust.lst_nm"
      ,"cust.mid_nm"
      ,"cust.acct_sfx_cd"
      ,"cust.brth_dt"
      ,"cust.acct_email_adr"
      ,"cust.wrk_email_adr"
      ,"cust.othr_email_adr"
      ,"cust.mlng_str_adr"
      ,"cust.mlng_cty_nm"
      ,"cust.mlng_st_cd"
      ,"cust.mlng_ctry_txt"
      ,"cust.mlng_pstl_cd"
      ,"cust.mobl_ph_nb"
      ,"cust.othr_str_adr"
      ,"cust.othr_cty_nm"
      ,"cust.oth_st_cd"
      ,"cust.othr_ctry_txt"
      ,"cust.othr_pstl_cd"
      ,"cust.othr_ph_nb"
      ,"cust.ph_nb"
      ,"cust.ctcd_dot_ind"
      ,"cust.excsv_comp_ind"
      ,"cust.frd_ind"
      ,"cust.no_fly_ind"
      ,"cust.dot_thrt_ind"
      ,"cust.wrng_lttr_ind"
      ,"cust.loyaltyMemberId"
      ,"cust.carrierCode"
      ,"cc.case_sfdc_case_id"
      ,"cc.case_nb"
      ,"case_orig_src_nm"
      ,"case_rec_typ_id"
      ,"case_clsd_ts"
      ,"case_crtd_ts"
      ,"case_orgl_ds_txt"
      ,"case_stt_nm"
      ,"case_srt_ds_txt"
      ,"case_typ_nm"
      ,"case_sfdc_usr_id"
      ,"dot_dsbly_rsn_txt"
      ,"case_shrt_ds_txt"
      ,"clm_amt"
      ,"dnd_bag_clm_rsn_ds"
      ,"baggageDisabilityItemText"
      ,"excld_rsn_txt"
      ,"excn_amt"
      ,"excn_rsn_txt"
      ,"ib_lang_cd"
      ,"init_case_rcvd_dt"
      ,"sfdc_comp_typ_id"
      ,"comp_typ_crtd_by_usr_id"
      ,"comp_crtd_ts"
      ,"comp_typ_lst_mdfd_ts"
      ,"comp_typ_acct_nb"
      ,"comp_typ_assd_sfdc_case_id"
      ,"uuid_txt"
      ,"comp_typ_ds"
      ,"comp_rsn_txt"
      ,"comp_amt"
      ,"curr_cd_txt"
      ,"sm_val_nb"
      ,"Gift_Bskt_Val_Txt"
      ,"rfnd_rsn_txt"
      ,"tkdc_nb"
      ,"bnk_nm"
      ,"bnk_adr"
      ,"bnk_ph_nb"
      ,"bnk_brch_nb"
      ,"branchName"
      ,"branchCode"
      ,"cust_nm"
      ,"comp_stt_cd"
      ).distinct()
      cust_case_comp_df.cache()

//      println("Printing Cust: Case:Compensation................................ ")
//      cust_case_comp_df.show()


    // Join CustCaseComp with PNR
    println("===============================================================")
    println("JOINING CustCaseCompensation with PNR ")
    println("===============================================================")
    val cust_case_comp_pnr_df = cust_case_comp_df.alias("cc").join(pnrtempdf1.alias("pnr"),$"cc.case_sfdc_case_id" === $"pnr.case_sfdc_case_id","left_outer").selectExpr(
      "cc.pcid"
      ,"cc.sfdc_acct_id"
      ,"cc.frst_nm"
      ,"cc.lst_nm"
      ,"cc.mid_nm"
      ,"cc.acct_sfx_cd"
      ,"cc.brth_dt"
      ,"cc.acct_email_adr"
      ,"cc.wrk_email_adr"
      ,"cc.othr_email_adr"
      ,"cc.mlng_str_adr"
      ,"cc.mlng_cty_nm"
      ,"cc.mlng_st_cd"
      ,"cc.mlng_ctry_txt"
      ,"cc.mlng_pstl_cd"
      ,"cc.mobl_ph_nb"
      ,"cc.othr_str_adr"
      ,"cc.othr_cty_nm"
      ,"cc.oth_st_cd"
      ,"cc.othr_ctry_txt"
      ,"cc.othr_pstl_cd"
      ,"cc.othr_ph_nb"
      ,"cc.ph_nb"
      ,"cc.ctcd_dot_ind"
      ,"cc.excsv_comp_ind"
      ,"cc.frd_ind"
      ,"cc.no_fly_ind"
      ,"cc.dot_thrt_ind"
      ,"cc.wrng_lttr_ind"
      ,"cc.loyaltyMemberId"
      ,"cc.carrierCode"
      ,"cc.case_sfdc_case_id"
      ,"cc.case_nb"
      ,"cc.case_orig_src_nm"
      ,"cc.case_rec_typ_id"
      ,"cc.case_clsd_ts"
      ,"cc.case_crtd_ts"
      ,"cc.case_orgl_ds_txt"
      ,"cc.case_stt_nm"
      ,"cc.case_srt_ds_txt"
      ,"cc.case_typ_nm"
      ,"cc.case_sfdc_usr_id"
      ,"cc.dot_dsbly_rsn_txt"
      ,"cc.case_shrt_ds_txt"
      ,"cc.clm_amt"
      ,"cc.dnd_bag_clm_rsn_ds"
      ,"cc.baggageDisabilityItemText"
      ,"cc.excld_rsn_txt"
      ,"cc.excn_amt"
      ,"cc.excn_rsn_txt"
      ,"cc.ib_lang_cd"
      ,"cc.init_case_rcvd_dt"
      ,"cc.sfdc_comp_typ_id"
      ,"cc.comp_typ_crtd_by_usr_id"
      ,"cc.comp_crtd_ts"
      ,"cc.comp_typ_lst_mdfd_ts"
      ,"cc.comp_typ_acct_nb"
      ,"cc.comp_typ_assd_sfdc_case_id"
      ,"cc.uuid_txt"
      ,"cc.comp_typ_ds"
      ,"cc.comp_rsn_txt"
      ,"cc.comp_amt"
      ,"cc.curr_cd_txt"
      ,"cc.sm_val_nb"
      ,"cc.Gift_Bskt_Val_Txt"
      ,"cc.rfnd_rsn_txt"
      ,"cc.tkdc_nb"
      ,"cc.bnk_nm"
      ,"cc.bnk_adr"
      ,"cc.bnk_ph_nb"
      ,"cc.bnk_brch_nb"
      ,"cc.branchName"
      ,"cc.branchCode"
      ,"cc.cust_nm"
      ,"cc.comp_stt_cd"
      ,"pnr.sfdc_pnr_id"
      ,"pnr.pnr_crtd_by_usr_id"
      ,"pnr.PNR_Lst_Mdfd_Ts"
      ,"pnr.PNR_SFDC_Case_Id"
      ,"pnr.PNR_Rec_Locr_Id"
    ).distinct()
    cust_case_comp_pnr_df.cache()

    println("Printing Cust:Case:Compensation:PNR................................ ")
//    cust_case_comp_pnr_df.show()

    // Join CustCaseCompPNR with TKT
    println("===============================================================")
    println("JOINING CustCaseCompensationPNR with TKT ")
    println("===============================================================")
    val cust_case_comp_pnr_tkt_df = cust_case_comp_pnr_df.alias("cc").join(tkttempdf1.alias("tkt"),$"cc.case_sfdc_case_id" === $"tkt.case_sfdc_case_id","left_outer").selectExpr(
      "cc.pcid"
      ,"cc.sfdc_acct_id"
      ,"cc.frst_nm"
      ,"cc.lst_nm"
      ,"cc.mid_nm"
      ,"cc.acct_sfx_cd"
      ,"cc.brth_dt"
      ,"cc.acct_email_adr"
      ,"cc.wrk_email_adr"
      ,"cc.othr_email_adr"
      ,"cc.mlng_str_adr"
      ,"cc.mlng_cty_nm"
      ,"cc.mlng_st_cd"
      ,"cc.mlng_ctry_txt"
      ,"cc.mlng_pstl_cd"
      ,"cc.mobl_ph_nb"
      ,"cc.othr_str_adr"
      ,"cc.othr_cty_nm"
      ,"cc.oth_st_cd"
      ,"cc.othr_ctry_txt"
      ,"cc.othr_pstl_cd"
      ,"cc.othr_ph_nb"
      ,"cc.ph_nb"
      ,"cc.ctcd_dot_ind"
      ,"cc.excsv_comp_ind"
      ,"cc.frd_ind"
      ,"cc.no_fly_ind"
      ,"cc.dot_thrt_ind"
      ,"cc.wrng_lttr_ind"
      ,"cc.loyaltyMemberId"
      ,"cc.carrierCode"
      ,"cc.case_sfdc_case_id"
      ,"cc.case_nb"
      ,"cc.case_orig_src_nm"
      ,"cc.case_rec_typ_id"
      ,"cc.case_clsd_ts"
      ,"cc.case_crtd_ts"
      ,"cc.case_orgl_ds_txt"
      ,"cc.case_stt_nm"
      ,"cc.case_srt_ds_txt"
      ,"cc.case_typ_nm"
      ,"cc.case_sfdc_usr_id"
      ,"cc.dot_dsbly_rsn_txt"
      ,"cc.case_shrt_ds_txt"
      ,"cc.clm_amt"
      ,"cc.dnd_bag_clm_rsn_ds"
      ,"cc.baggageDisabilityItemText"
      ,"cc.excld_rsn_txt"
      ,"cc.excn_amt"
      ,"cc.excn_rsn_txt"
      ,"cc.ib_lang_cd"
      ,"cc.init_case_rcvd_dt"
      ,"cc.sfdc_comp_typ_id"
      ,"cc.comp_typ_crtd_by_usr_id"
      ,"cc.comp_crtd_ts"
      ,"cc.comp_typ_lst_mdfd_ts"
      ,"cc.comp_typ_acct_nb"
      ,"cc.comp_typ_assd_sfdc_case_id"
      ,"cc.uuid_txt"
      ,"cc.comp_typ_ds"
      ,"cc.comp_rsn_txt"
      ,"cc.comp_amt"
      ,"cc.curr_cd_txt"
      ,"cc.sm_val_nb"
      ,"cc.Gift_Bskt_Val_Txt"
      ,"cc.rfnd_rsn_txt"
      ,"cc.tkdc_nb"
      ,"cc.bnk_nm"
      ,"cc.bnk_adr"
      ,"cc.bnk_ph_nb"
      ,"cc.bnk_brch_nb"
      ,"cc.branchName"
      ,"cc.branchCode"
      ,"cc.cust_nm"
      ,"cc.comp_stt_cd"
      ,"cc.sfdc_pnr_id"
      ,"cc.pnr_crtd_by_usr_id"
      ,"cc.PNR_Lst_Mdfd_Ts"
      ,"cc.PNR_SFDC_Case_Id"
      ,"cc.PNR_Rec_Locr_Id"
      ,"tkt.sfdc_tkt_id"
      ,"tkt.TKT_Crtd_By_Usr_Id"
      ,"tkt.TKT_Lst_Mdfd_Ts"
      ,"tkt.TKT_SFDC_Case_Id"
      ,"tkt.Tkt_Doc_Nb"
      ,"tkt.Tkt_Dsgtr_Cd"
      ,"tkt.seat_id"
    ).distinct()
//    cust_case_comp_pnr_tkt_df.cache()

    println("Printing Cust:Case:Compensation:PNR:TKT................................ ")
//    cust_case_comp_pnr_tkt_df.show()

//     Join CustCaseCompPNRTKT with FLTLEG
    println("===============================================================")
    println("JOINING CustCaseCompensationPNRTKT with FLTLEG ")
    println("===============================================================")
    val cust_case_comp_pnr_tkt_flt_df = cust_case_comp_pnr_tkt_df.alias("cc").join(dffleg.alias("flt"),$"cc.case_sfdc_case_id" === $"flt.flt_sfdc_case_id","left_outer").selectExpr(
      "cc.pcid"
      ,"cc.sfdc_acct_id"
      ,"cc.frst_nm"
      ,"cc.lst_nm"
      ,"cc.mid_nm"
      ,"cc.acct_sfx_cd"
      ,"cc.brth_dt"
      ,"cc.acct_email_adr"
      ,"cc.wrk_email_adr"
      ,"cc.othr_email_adr"
      ,"cc.mlng_str_adr"
      ,"cc.mlng_cty_nm"
      ,"cc.mlng_st_cd"
      ,"cc.mlng_ctry_txt"
      ,"cc.mlng_pstl_cd"
      ,"cc.mobl_ph_nb"
      ,"cc.othr_str_adr"
      ,"cc.othr_cty_nm"
      ,"cc.oth_st_cd"
      ,"cc.othr_ctry_txt"
      ,"cc.othr_pstl_cd"
      ,"cc.othr_ph_nb"
      ,"cc.ph_nb"
      ,"cc.ctcd_dot_ind"
      ,"cc.excsv_comp_ind"
      ,"cc.frd_ind"
      ,"cc.no_fly_ind"
      ,"cc.dot_thrt_ind"
      ,"cc.wrng_lttr_ind"
      ,"cc.loyaltyMemberId"
      ,"cc.carrierCode"
      ,"cc.case_sfdc_case_id"
      ,"cc.case_nb"
      ,"cc.case_orig_src_nm"
      ,"cc.case_rec_typ_id"
      ,"cc.case_clsd_ts"
      ,"cc.case_crtd_ts"
      ,"cc.case_orgl_ds_txt"
      ,"cc.case_stt_nm"
      ,"cc.case_srt_ds_txt"
      ,"cc.case_typ_nm"
      ,"cc.case_sfdc_usr_id"
      ,"cc.dot_dsbly_rsn_txt"
      ,"cc.case_shrt_ds_txt"
      ,"cc.clm_amt"
      ,"cc.dnd_bag_clm_rsn_ds"
      ,"cc.baggageDisabilityItemText"
      ,"cc.excld_rsn_txt"
      ,"cc.excn_amt"
      ,"cc.excn_rsn_txt"
      ,"cc.ib_lang_cd"
      ,"cc.init_case_rcvd_dt"
      ,"cc.sfdc_comp_typ_id"
      ,"cc.comp_typ_crtd_by_usr_id"
      ,"cc.comp_crtd_ts"
      ,"cc.comp_typ_lst_mdfd_ts"
      ,"cc.comp_typ_acct_nb"
      ,"cc.comp_typ_assd_sfdc_case_id"
      ,"cc.uuid_txt"
      ,"cc.comp_typ_ds"
      ,"cc.comp_rsn_txt"
      ,"cc.comp_amt"
      ,"cc.curr_cd_txt"
      ,"cc.sm_val_nb"
      ,"cc.Gift_Bskt_Val_Txt"
      ,"cc.rfnd_rsn_txt"
      ,"cc.tkdc_nb"
      ,"cc.bnk_nm"
      ,"cc.bnk_adr"
      ,"cc.bnk_ph_nb"
      ,"cc.bnk_brch_nb"
      ,"cc.branchName"
      ,"cc.branchCode"
      ,"cc.cust_nm"
      ,"cc.comp_stt_cd"
      ,"cc.sfdc_pnr_id"
      ,"cc.pnr_crtd_by_usr_id"
      ,"cc.PNR_Lst_Mdfd_Ts"
      ,"cc.PNR_SFDC_Case_Id"
      ,"cc.PNR_Rec_Locr_Id"
      ,"cc.sfdc_tkt_id"
      ,"cc.TKT_Crtd_By_Usr_Id"
      ,"cc.TKT_Lst_Mdfd_Ts"
      ,"cc.TKT_SFDC_Case_Id"
      ,"cc.Tkt_Doc_Nb"
      ,"cc.Tkt_Dsgtr_Cd"
      ,"cc.seat_id"
      ,"flt.flightleg"
    ).distinct()
//    cust_case_comp_pnr_tkt_flt_df.cache()

    println("Printing Cust:Case:Compensation:PNR:TKT:FLTLEG................................ ")
    cust_case_comp_pnr_tkt_flt_df.printSchema()

    println("===============================================================")
    println("Loading CustCaseCompensation into Curated ")
    println("===============================================================")
//    loadCompCurated(cust_case_comp_pnr_tkt_flt_df)

    println("===============================================================")
    println("Loading PNR to Curated")
    println("===============================================================")
//    loadPNRCurated(cust_case_comp_pnr_tkt_flt_df)

    println("===============================================================")
    println("Loading TKT to Curated")
    println("===============================================================")
//    loadTKTCurated(cust_case_comp_pnr_tkt_flt_df)

    println("===============================================================")
    println("Loading Feedback to Curated")
    println("===============================================================")
//    loadFDBKCurated(cust_case_comp_df)

    println("===============================================================")
    println("Loading Published  to Published")
    println("===============================================================")
    loadPublished(cust_case_comp_pnr_tkt_flt_df)

  }
//
  //Load data to Curated HBase table
  def loadCompCurated (df1 : DataFrame) : Unit = {


    println("Generate curated data for Compensation.....")
    df1.createOrReplaceTempView("phx_compensation_temp1")
    val dfcompcurated = spark.sql("SELECT  concat(case_sfdc_case_id, '|', sfdc_comp_typ_id),  pcid,  sfdc_acct_id,  case_sfdc_case_id,  frst_nm,  lst_nm,  mid_nm,  acct_sfx_cd,  brth_dt,  acct_email_adr,  wrk_email_adr,  othr_email_adr,  mlng_str_adr,  mlng_cty_nm,  mlng_st_cd,  mlng_ctry_txt,  mlng_pstl_cd,  mobl_ph_nb,  othr_str_adr,  othr_cty_nm,  oth_st_cd,  othr_ctry_txt,  othr_pstl_cd,  othr_ph_nb,  ph_nb,  ctcd_dot_ind,  excsv_comp_ind,  frd_ind,  no_fly_ind,  dot_thrt_ind,  wrng_lttr_ind,  " +
      "loyaltyMemberId as SM_Loy_Pgm_Acct_Id, '' as fb_nb,'' as oa_loy_pgm_nm ,'' as oa_loy_pgm_id, case_sfdc_case_id,  case_nb,  case_orig_src_nm,  case_rec_typ_id,  case_typ_nm,  case_sfdc_usr_id,  case_clsd_ts,  case_crtd_ts,  case_orgl_ds_txt,  case_stt_nm,  case_shrt_ds_txt, clm_amt,dnd_bag_clm_rsn_ds , '' asdot_dsbly_rsn_txt, excld_rsn_txt,  excn_amt,  excn_rsn_txt, '' as fstk_al_cd_nb, ib_lang_cd,init_case_rcvd_dt,' ' as baggage_ind, sfdc_comp_typ_id,  " +
      "comp_typ_crtd_by_usr_id,  comp_crtd_ts,  comp_typ_lst_mdfd_ts,  ' ' as comp_typ_lst_mdfd_usr_id,  comp_typ_acct_nb,  " +
      "comp_typ_assd_sfdc_case_id,  uuid_txt,  '' as comp_typ_rec_typ_id,comp_typ_ds,  comp_rsn_txt,  comp_amt,  comp_amt as chk_amt,  " +
      "comp_amt as etcv_amt,  comp_amt as dl_chc_amt,  sm_val_nb as sm_val_nb,  sm_val_nb as Fb_Mls_Ct,  '' as Qty_Nb,  Gift_Bskt_Val_Txt,  " +
      "'' as gift_bskt_curr_cd,  '' as tot_ECV_Amt,  '' as chk_nb,  curr_cd_txt,  '' as gift_bskt_msg_txt,  rfnd_rsn_txt,  bnk_nm,  bnk_adr,  " +
      "bnk_ph_nb,  bnk_brch_nb,  '' as nm_on_acct_txt,  '' as exctn_dt,  comp_stt_cd,  comp_crtd_ts as stt_updt_ts,  cust_nm,  tkdc_nb,  " +
      "'' as void_apvd_ind,  '' as void_rsn_txt,  '' as str_adr,  '' as sfdc_case_ctac_id,  '' as rlnshp_usr_creation_id,  " +
      "'' as rlnshp_created_ts,  '' as obj_dld_ind,  '' as lst_atvy_dt,  '' as rlnshp_modified_user_id,  '' as rlnshp_modified_ts,  " +
      "'' as rlnshp_lst_rfrnd_ts,  '' as rlnshp_lst_vwd_ts,  '' as case_ctac_rshp_typ_cd,  'PRI' as relationship_name,'' as rlnshp_sys_mdfcn_ts,'' as case_ctac_rep_ind," +
      "'' as rlnshp_rec_ld_gts,comp_typ_crtd_by_usr_id as ppr_nb,'' as Gift_Bskt_Cur_cd,'' as dl_chc_curr_txt from phx_compensation_temp1 ").distinct().toDF()
//    dfcompcurated.printSchema()
//    dfcompcurated.show()
    dfcompcurated.createOrReplaceTempView("phx_compensation_temp2")
    spark.sql("SELECT * FROM phx_compensation_temp2")
    spark.sql("INSERT OVERWRITE TABLE svoc_stage_dev.phx_compensation_rt_temp SELECT * FROM phx_compensation_temp2")
  }

  //Load data to Curated HBase table
  def loadPNRCurated (df1 : DataFrame) : Unit = {
    println("Generate curated data for PNR.....")

    df1.createOrReplaceTempView("phx_pnr_temp1")
    val dfpnrcurated = spark.sql("SELECT  concat(case_sfdc_case_id, '|', sfdc_pnr_id),  pcid,  sfdc_acct_id,  case_sfdc_case_id, sfdc_pnr_id,pnr_crtd_by_usr_id,PNR_Lst_Mdfd_Ts,'' as sfdc_pnr_nm,PNR_SFDC_Case_Id,'' as PNR_Lst_Mdfd_Usr_Id,PNR_Rec_Locr_Id,'' as PNR_bkg_crtn_dt from phx_pnr_temp1").distinct().toDF()
//    dfpnrcurated.printSchema()
    dfpnrcurated.show()
    dfpnrcurated.createOrReplaceTempView("phx_pnr_temp2")
    spark.sql("""INSERT OVERWRITE TABLE svoc_stage_dev.phx_pnr_rt_temp SELECT * FROM phx_pnr_temp2""")
  }

  //Load data to Curated HBase table
  def loadTKTCurated (df1 : DataFrame) : Unit = {
    println("Generate curated data for TKT.....")
    df1.createOrReplaceTempView("phx_tkt_temp1")
    val dftktcurated = spark.sql("SELECT  concat(case_sfdc_case_id, '|', sfdc_tkt_id) as tbl_row_id,  pcid,  sfdc_acct_id,  case_sfdc_case_id," +
      "sfdc_tkt_id,TKT_Crtd_By_Usr_Id,TKT_Lst_Mdfd_Ts,'' as TKT_Lst_Mdfd_Usr_Id,TKT_SFDC_Case_Id,Tkt_Doc_Nb,'' as Tkt_Doc_Iss_Dt," +
      "'' as fare_bas_cd,Tkt_Dsgtr_Cd,seat_id,'' as tkt_nm from phx_tkt_temp1").distinct().toDF()
//    dftktcurated.printSchema()
    dftktcurated.show()
    dftktcurated.createOrReplaceTempView("phx_tkt_temp2")
    spark.sql("""INSERT OVERWRITE TABLE svoc_stage_dev.phx_tkt_rt_temp SELECT * FROM phx_tkt_temp2""")
  }

  //Load data to Curated HBase table
//  def loadFDBKCurated (df1 : DataFrame) : Unit = {
//    println("Generate curated data for FDBK.....")
//    df1.createOrReplaceTempView("phx_compensation_temp")
//    val dffdbkcurated = spark.sql("SELECT  concat(case_sfdc_case_id, '|', sfdc_fdbk_id),   from phx_feedback_temp")
//    dffdbkcurated.printSchema()
//    dffdbkcurated.show()
//  }

//  Load data to Curated HBase table
  def loadPublished (df6 : DataFrame) : Unit = {
    println("Generate Published.....")
    df6.printSchema()
    df6.createOrReplaceTempView("phx_published_temp1")
    spark.sql("SELECT concat( PCID,'|',case_crtd_ts,'|','PHX','|', sfdc_comp_typ_id) as tbl_row_id, pcid  ,case_crtd_ts,concat('PHX','|', sfdc_comp_typ_id) as Comp_Evt_Id ,'Care' as Comp_Req_Chnl_Nm,case_sfdc_usr_id as Agt_Id,case_sfdc_usr_id as PPR_Nb, cust_nm as Cust_Full_Nm , sfdc_acct_id  ,  frst_nm  ,  lst_nm  ,  mid_nm  ,  case when  trim( comp_typ_ds) = 'SkyMiles' and trim( loyaltyMemberId) is not null and trim( loyaltyMemberId) <> 'null' then 'DL' when  trim( comp_typ_ds) = 'Flying Blue Miles' and trim( FB_Nb ) <> 'null' and trim( FB_Nb) is not null then 'AF'  else ''end as Comp_Evt_Loy_Pgm_Crr_Cd,case when trim( comp_typ_ds) = 'SkyMiles' then  loyaltyMemberId      when trim( comp_typ_ds) = 'Flying Blue Miles' then  FB_Nb else '' end as Comp_Evt_Loy_Pgm_Acct_Id, Acct_Email_Adr as Comp_Evt_Email_Adr, Ph_Nb as Comp_Evt_Ph_Nb, mlng_str_adr as Comp_Evt_Gphc_Adr_Ln_1_Txt,'' as Comp_Evt_Gphc_Adr_Ln_2_Txt,acct_sfx_cd  ,  brth_dt  ,  acct_email_adr  ,  wrk_email_adr  ,  othr_email_adr  ,  mlng_str_adr  ,  mlng_cty_nm  ,  mlng_st_cd  ,  mlng_ctry_txt  ,  mlng_pstl_cd  ,  mobl_ph_nb  ,  othr_str_adr  ,  othr_cty_nm  ,  oth_st_cd  ,  othr_ctry_txt  ,  othr_pstl_cd  ,  othr_ph_nb  ,  ph_nb  ,  ctcd_dot_ind  ,  excsv_comp_ind  ,  frd_ind  ,  no_fly_ind  ,  dot_thrt_ind  ,  wrng_lttr_ind  ,  loyaltyMemberId  ,  carrierCode  ,  case_sfdc_case_id  ,  case_nb  ,  case_orig_src_nm  ,  case_rec_typ_id  ,  case_clsd_ts  ,  case_crtd_ts  ,  case_orgl_ds_txt  ,  case_stt_nm  ,  case_srt_ds_txt  ,  case_typ_nm  ,  case_sfdc_usr_id  ,  dot_dsbly_rsn_txt  ,  case_shrt_ds_txt  ,  clm_amt  ,  dnd_bag_clm_rsn_ds  ,  baggageDisabilityItemText  ,  excld_rsn_txt  ,  excn_amt  ,  excn_rsn_txt  ,  ib_lang_cd  ,  init_case_rcvd_dt  ,  sfdc_comp_typ_id  ,  comp_typ_crtd_by_usr_id  ,  comp_crtd_ts  ,  comp_typ_lst_mdfd_ts  ,  comp_typ_acct_nb  ,  comp_typ_assd_sfdc_case_id  ,  uuid_txt  ,  comp_typ_ds  ,  comp_rsn_txt  ,  comp_amt  ,  curr_cd_txt  ,  sm_val_nb  ,  Gift_Bskt_Val_Txt  ,  rfnd_rsn_txt  ,  tkdc_nb  ,  bnk_nm  ,  bnk_adr  ,  bnk_ph_nb  ,  bnk_brch_nb  ,  branchName  ,  branchCode  ,    comp_stt_cd  ,  sfdc_pnr_id  ,  pnr_crtd_by_usr_id  ,  PNR_Lst_Mdfd_Ts  ,  PNR_SFDC_Case_Id  ,  PNR_Rec_Locr_Id  ,  sfdc_tkt_id  ,  TKT_Crtd_By_Usr_Id  ,  TKT_Lst_Mdfd_Ts  ,  TKT_SFDC_Case_Id  ,  Tkt_Doc_Nb  ,  Tkt_Dsgtr_Cd  ,  seat_id,flightleg   FROM phx_published_temp1").show()
//    val dfpublished = spark.sql(" select distinct " +
////      "concat(ac.PCID,'|',date_format(ac.case_crtd_ts,'yyyy-MM-dd'T'hh:mm:ss.SSS'Z''),'|','PHX','|',ac.sfdc_comp_typ_id) as tbl_row_id," +
//      "ac.pcid,date_format(ac.case_crtd_ts,'yyyy-MM-dd'T'hh:mm:ss.SSS'Z'') as Comp_Crtd_TS,concat('PHX','|',ac.sfdc_comp_typ_id) as Comp_Evt_Id," +
//      "'Care' as Comp_Req_Chnl_Nm,ac.case_sfdc_usr_id as Agt_Id,ac.case_sfdc_usr_id as PPR_Nb,ac.cust_nm as Cust_Full_Nm,ac.frst_nm as Cust_Frst_Nm," +
//      "ac.mid_nm as Cust_Mid_Nm,ac.lst_nm as Cust_Lst_Nm,case when  trim(ac.comp_typ_ds) = 'SkyMiles' " +
//      "and trim(ac.SM_Loy_Pgm_Acct_Id) is not null and trim(ac.SM_Loy_Pgm_Acct_Id) <> 'null' then 'DL' when  trim(ac.comp_typ_ds) = 'Flying Blue Miles' " +
//      "and trim(ac.FB_Nb ) <> 'null' and trim(ac.FB_Nb) is not null then 'AF'  else ''end as Comp_Evt_Loy_Pgm_Crr_Cd,case " +
//      "when trim(ac.comp_typ_ds) = 'SkyMiles' then ac.SM_Loy_Pgm_Acct_Id      when trim(ac.comp_typ_ds) = 'Flying Blue Miles' " +
//      "then ac.FB_Nb else '' end as Comp_Evt_Loy_Pgm_Acct_Id,ac.Acct_Email_Adr as Comp_Evt_Email_Adr," +
//      "ac.Ph_Nb as Comp_Evt_Ph_Nb,ac.mlng_str_adr as Comp_Evt_Gphc_Adr_Ln_1_Txt,'' as Comp_Evt_Gphc_Adr_Ln_2_Txt," +
//      "ac.Mlng_Cty_Nm as Comp_Evt_Cty_Nm,ac.Mlng_St_Cd as Comp_Evt_St_Cd,ac.Mlng_Ctry_Txt as Comp_Evt_Ctry_Cd, " +
//      " '' as Comp_Evt_Pstl_Area_Cd,'' as Vlry_Invlt_Ind, ''as Cmlt_Rsn_Txt," +
//      "case when ac.Comp_Rsn_Txt in ('Goodwill Gesture', 'Customer Inconvenience' , 'Fulfillment','FPOC Res','FPOC ACS') then 'Goodwill Gesture'  " +
//      " when ac.Comp_Rsn_Txt = 'Customer Changes Seat' then 'Seat Issue'when ac.Comp_Rsn_Txt = 'Meal Shortage' then 'Meal Service Issue'when ac.Comp_Rsn_Txt = 'Lack of Special Meal' then 'Meal Service Issue'" +
//      " when ac.Comp_Rsn_Txt = 'Medical' then 'Medical Assist'when ac.Comp_Rsn_Txt = ' IFE issues < 20' then 'IFE Issue'when ac.Comp_Rsn_Txt = ' EU' then 'EU Regulation' " +
//      " when ac.Comp_Rsn_Txt = 'Israel ASL' then 'Israel Regulation'when ac.Comp_Rsn_Txt = 'Reimbursement' then 'Out of Pocket Expenses'  " +
//      "when ac.Comp_Rsn_Txt = 'Employee Issue' then 'Employee Behavior'when ac.Comp_Rsn_Txt = 'ANAC Directive 141' then 'Brazil Regulation' " +
//      "when ac.Comp_Rsn_Txt = 'Chile Passenger Protection' then 'Chile Regulation' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance Chile' then 'Care and Assistance Chile' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance EU' then 'Care and Assistance EU' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance Israel' then 'Care and Assistance Israel' " +
//      "else 'Goodwill Gesture'end Comp_Rsn_Txt,'' as Prmo_Nm,'' as Wavr_Cd," +
//      "case when ac.comp_typ_ds = 'ECV' then 'TCV'when ac.comp_typ_ds in ('Cash','Check', 'International Payments') then 'Monetary' " +
//      "when ac.comp_typ_ds in ('Delta Choices','Gift Card') then 'Gift Card' " +
//      "when ac.comp_typ_ds = 'Inflight Coupons' then 'Drink Coupons'when ac.comp_typ_ds = 'Transportation Voucher' then 'Ground Transportation' " +
//      "when ac.comp_typ_ds = 'Food Voucher' then 'Meal Voucher' " +
//      " when ac.comp_typ_ds = 'Refunds' then 'Refund' " +
//      " when ac.comp_typ_ds = 'Gift Baskets' then 'Gift Basket'" +
//      " when ac.comp_typ_ds = 'SkyMiles' then 'Sky Miles'else ac.comp_typ_ds end as Comp_Typ_Cd," +
//      "case when ac.Comp_Stt_Typ_Cd is not null then ac.Comp_Stt_Typ_Cd when ac.comp_typ_ds = 'SkyMiles' then  'REQUESTED'  " +
//      "when ac.comp_typ_ds = 'ECV' then 'REQUESTED'  " +
//      "when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd in('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd = 'Completed' and trim(ac.tkdc_nb) is not null then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd = 'Completed' and trim(ac.tkdc_nb) is null then 'REQUESTED' " +
//      "when ac.comp_typ_ds = 'Gift Basket' and ac.comp_stt_cd in ('Completed') then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Gift Basket' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Inflight Coupons' and ac.comp_stt_cd in ('Completed') then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Inflight Coupons' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Flying Blue Miles' then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'International Payment' then 'REQUESTED'when ac.comp_typ_ds = 'Sky Club Pass' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Sky Club Pass' and ac.comp_stt_cd = 'Completed' then  'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Delta Choices' then 'REQUESTED'when ac.comp_typ_ds = 'Refunds' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED' " +
//      " when ac.comp_typ_ds = 'Wire Transfer' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED' " +
//      " when ac.comp_typ_ds in ('Wire Transfer','Flying Blue Miles','Gift Basket','Refunds','Check','Inflight Coupons','Sky Club Pass') and ac.comp_stt_cd in ('Failed') then 'REJECTED' " +
//      "when ac.comp_typ_ds in ('Wire Transfer','Refunds','Check') and ac.comp_stt_cd in ('Void') then 'VOIDED' else 'REQUESTED'end as Comp_Stt_Typ_Cd    ," +
//      " ac.Comp_Typ_Lst_Mdfd_Ts as Comp_Stt_LTs," +
//      " case when ac.comp_typ_ds = 'ECV' then ac.ETCV_Amt when ac.comp_typ_ds = 'International Payment' then ac.Comp_Amt " +
//      " when ac.comp_typ_ds = 'Check' then ac.chk_amt when ac.comp_typ_ds in ('Delta Choices','Gift Card') then ac.dl_chc_amt " +
//      " when ac.comp_typ_ds = 'Refund' then ac.Comp_Amt" +
//      " when ac.comp_typ_ds = 'Gift Basket'  then ac.Gift_Bskt_Val_Txt " +
//      " when ac.comp_typ_ds = 'Inflight Coupons'  then 10* nvl(ac.Qty_Nb,0) " +
//      " when ac.comp_typ_ds = 'Sky Club Pass'  then 50* nvl(ac.Qty_Nb,0) else ac.Comp_Amt end as Comp_Amt,case " +
//      "when ac.Comp_Curr_Cd is not null then ac.Comp_Curr_Cd " +
//      " when  ac.comp_typ_ds ='Gift Basket' then ac.Gift_Bskt_Cur_cd      " +
//      " when ac.comp_typ_ds in ('Delta Choices','Gift Card') then ac.dl_chc_curr_txt " +
//      " when ac.comp_typ_ds in ('Check') then ac.curr_cd_txt " +
//      " when ac.comp_typ_ds = 'International Payment' then 'USD' " +
//      " else ac.curr_cd_txt end as Comp_Curr_Cd,case when trim(ac.comp_typ_ds) = 'SkyMiles' then ac.sm_val_nb      " +
//      "when  ac.comp_typ_ds = 'Flying Blue Miles' then ac.Fb_Mls_Ct end as Comp_Mi_Ct,ac.case_nb as Chnl_Case_Nb," +
//      "case when ac.comp_typ_ds in ('ECV') then ac.uuid_txt else '' end as EMD_Tkt_Doc_Nb," +
//      "case when ac.comp_typ_ds in ('Check') then ac.chk_nb when  ac.comp_typ_ds in ('Delta Choices') then ac.uuid_txt end as TRNS_RFRN_CD," +
//      "'' as Comp_Dlry_Meth_Typ_Cd,'' as Comp_Dlry_UTs,'' OpAs_Crr_Cd,'' as Mkd_Crr_Cd," +
//      " ac.PNR_Rec_Locr_Id as Rec_Locr_Id,'' as Bkg_Crtn_GTs,'' as SSR_Ind,'' as Tkt_Doc_Nb," +
//      " '' as Tkt_Doc_Iss_Dt ,'' as Comp_Flt_Legs from phx_published_temp1 ac ").distinct().toDF()
//    " '' as Comp_Flt_Legs from phx_published_temp1 ac ").distinct().toDF()

//    val dfpublished = spark.sql("INSERT OVERWRITE TABLE svoc_stage_dev.phx_customer_compensation_rt_temp select distinct " +
//      "concat(ac.PCID,'|',date_format(ac.case_crtd_ts,'yyyy-MM-dd'T'hh:mm:ss.SSS'Z''),'|','PHX','|',ac.sfdc_comp_typ_id) as tbl_row_id," +
//      "ac.pcid,date_format(ac.case_crtd_ts,'yyyy-MM-dd'T'hh:mm:ss.SSS'Z'') as Comp_Crtd_TS,concat('PHX','|',ac.sfdc_comp_typ_id) as Comp_Evt_Id," +
//      "'Care' as Comp_Req_Chnl_Nm,ac.case_sfdc_usr_id as Agt_Id,ac.case_sfdc_usr_id as PPR_Nb,ac.cust_nm as Cust_Full_Nm,ac.frst_nm as Cust_Frst_Nm," +
//      "ac.mid_nm as Cust_Mid_Nm,ac.lst_nm as Cust_Lst_Nm,case when  trim(ac.comp_typ_ds) = 'SkyMiles' " +
//      "and trim(ac.SM_Loy_Pgm_Acct_Id) is not null and trim(ac.SM_Loy_Pgm_Acct_Id) <> 'null' then 'DL' when  trim(ac.comp_typ_ds) = 'Flying Blue Miles' " +
//      "and trim(ac.FB_Nb ) <> 'null' and trim(ac.FB_Nb) is not null then 'AF'  else ''end as Comp_Evt_Loy_Pgm_Crr_Cd,case " +
//      "when trim(ac.comp_typ_ds) = 'SkyMiles' then ac.SM_Loy_Pgm_Acct_Id      when trim(ac.comp_typ_ds) = 'Flying Blue Miles' " +
//      "then ac.FB_Nb else '' end as Comp_Evt_Loy_Pgm_Acct_Id,ac.Acct_Email_Adr as Comp_Evt_Email_Adr," +
//      "ac.Ph_Nb as Comp_Evt_Ph_Nb,ac.mlng_str_adr as Comp_Evt_Gphc_Adr_Ln_1_Txt,'' as Comp_Evt_Gphc_Adr_Ln_2_Txt," +
//      "ac.Mlng_Cty_Nm as Comp_Evt_Cty_Nm,ac.Mlng_St_Cd as Comp_Evt_St_Cd,ac.Mlng_Ctry_Txt as Comp_Evt_Ctry_Cd," +
//      "ac.Mlng_Pstl_Cd as Comp_Evt_Pstl_Area_Cd,'' as Vlry_Invlt_Ind,'' as Cmlt_Rsn_Txt," +
//      "case when ac.Comp_Rsn_Txt in ('Goodwill Gesture', 'Customer Inconvenience' , 'Fulfillment','FPOC Res','FPOC ACS') then 'Goodwill Gesture'  " +
//      " when ac.Comp_Rsn_Txt = 'Customer Changes Seat' then 'Seat Issue'when ac.Comp_Rsn_Txt = 'Meal Shortage' then 'Meal Service Issue'when ac.Comp_Rsn_Txt = 'Lack of Special Meal' then 'Meal Service Issue'" +
//      " when ac.Comp_Rsn_Txt = 'Medical' then 'Medical Assist'when ac.Comp_Rsn_Txt = ' IFE issues < 20' then 'IFE Issue'when ac.Comp_Rsn_Txt = ' EU' then 'EU Regulation' " +
//      " when ac.Comp_Rsn_Txt = 'Israel ASL' then 'Israel Regulation'when ac.Comp_Rsn_Txt = 'Reimbursement' then 'Out of Pocket Expenses'  " +
//      "when ac.Comp_Rsn_Txt = 'Employee Issue' then 'Employee Behavior'when ac.Comp_Rsn_Txt = 'ANAC Directive 141' then 'Brazil Regulation' " +
//      "when ac.Comp_Rsn_Txt = 'Chile Passenger Protection' then 'Chile Regulation' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance Chile' then 'Care and Assistance Chile' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance EU' then 'Care and Assistance EU' " +
//      " when ac.Comp_Rsn_Txt = 'Care & Assistance Israel' then 'Care and Assistance Israel' " +
//      "else 'Goodwill Gesture'end Comp_Rsn_Txt,'' as Prmo_Nm,'' as Wavr_Cd," +
//      "case when ac.comp_typ_ds = 'ECV' then 'TCV'when ac.comp_typ_ds in ('Cash','Check', 'International Payments') then 'Monetary' " +
//      "when ac.comp_typ_ds in ('Delta Choices','Gift Card') then 'Gift Card' " +
//      "when ac.comp_typ_ds = 'Inflight Coupons' then 'Drink Coupons'when ac.comp_typ_ds = 'Transportation Voucher' then 'Ground Transportation' " +
//      "when ac.comp_typ_ds = 'Food Voucher' then 'Meal Voucher' " +
//      " when ac.comp_typ_ds = 'Refunds' then 'Refund' " +
//      " when ac.comp_typ_ds = 'Gift Baskets' then 'Gift Basket'" +
//      " when ac.comp_typ_ds = 'SkyMiles' then 'Sky Miles'else ac.comp_typ_ds end as Comp_Typ_Cd," +
//      "case when ac.Comp_Stt_Typ_Cd is not null then ac.Comp_Stt_Typ_Cd when ac.comp_typ_ds = 'SkyMiles' then  'REQUESTED'  " +
//      "when ac.comp_typ_ds = 'ECV' then 'REQUESTED'  " +
//      "when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd in('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd = 'Completed' and trim(ac.tkdc_nb) is not null then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Check' and ac.comp_stt_cd = 'Completed' and trim(ac.tkdc_nb) is null then 'REQUESTED' " +
//      "when ac.comp_typ_ds = 'Gift Basket' and ac.comp_stt_cd in ('Completed') then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Gift Basket' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Inflight Coupons' and ac.comp_stt_cd in ('Completed') then 'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Inflight Coupons' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Flying Blue Miles' then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'International Payment' then 'REQUESTED'when ac.comp_typ_ds = 'Sky Club Pass' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED'" +
//      " when ac.comp_typ_ds = 'Sky Club Pass' and ac.comp_stt_cd = 'Completed' then  'FULFILLED'" +
//      " when ac.comp_typ_ds = 'Delta Choices' then 'REQUESTED'when ac.comp_typ_ds = 'Refunds' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED' " +
//      " when ac.comp_typ_ds = 'Wire Transfer' and ac.comp_stt_cd in ('Requested','In-Progress') then 'REQUESTED' " +
//      " when ac.comp_typ_ds in ('Wire Transfer','Flying Blue Miles','Gift Basket','Refunds','Check','Inflight Coupons','Sky Club Pass') and ac.comp_stt_cd in ('Failed') then 'REJECTED' " +
//      "when ac.comp_typ_ds in ('Wire Transfer','Refunds','Check') and ac.comp_stt_cd in ('Void') then 'VOIDED' else 'REQUESTED'end as Comp_Stt_Typ_Cd    ," +
//      " case when ac.Comp_Stt_LTs is not null then ac.Comp_Stt_LTs else ac.Comp_Typ_Lst_Mdfd_Ts end as Comp_Stt_LTs," +
//      " case when ac.comp_typ_ds = 'ECV' then ac.ETCV_Amt when ac.comp_typ_ds = 'International Payment' then ac.Comp_Amt " +
//      " when ac.comp_typ_ds = 'Check' then ac.chk_amt when ac.comp_typ_ds in ('Delta Choices','Gift Card') then ac.dl_chc_amt " +
//      " when ac.comp_typ_ds = 'Refund' then ac.Comp_Amt" +
//      " when ac.comp_typ_ds = 'Gift Basket'  then ac.Gift_Bskt_Val_Txt " +
//      " when ac.comp_typ_ds = 'Inflight Coupons'  then 10* nvl(ac.Qty_Nb,0) " +
//      " when ac.comp_typ_ds = 'Sky Club Pass'  then 50* nvl(ac.Qty_Nb,0) else ac.Comp_Amt end as Comp_Amt,case " +
//      "when ac.Comp_Curr_Cd is not null then ac.Comp_Curr_Cd " +
//      " when  ac.comp_typ_ds ='Gift Basket' then ac.Gift_Bskt_Cur_cd      " +
//      " when ac.comp_typ_ds in ('Delta Choices','Gift Card') then ac.dl_chc_curr_txt " +
//      " when ac.comp_typ_ds in ('Check') then ac.curr_cd_txt " +
//      " when ac.comp_typ_ds = 'International Payment' then 'USD' " +
//      " else ac.curr_cd_txt end as Comp_Curr_Cd,case when trim(ac.comp_typ_ds) = 'SkyMiles' then ac.sm_val_nb      " +
//      "when  ac.comp_typ_ds = 'Flying Blue Miles' then ac.Fb_Mls_Ct end as Comp_Mi_Ct,ac.case_nb as Chnl_Case_Nb," +
//      "case when ac.comp_typ_ds in ('ECV') then ac.uuid_txt else '' end as EMD_Tkt_Doc_Nb," +
//      "case when ac.comp_typ_ds in ('Check') then ac.chk_nb when  ac.comp_typ_ds in ('Delta Choices') then ac.uuid_txt end as TRNS_RFRN_CD," +
//      "'' as Comp_Dlry_Meth_Typ_Cd,'' as Comp_Dlry_UTs,'' OpAs_Crr_Cd,'' as Mkd_Crr_Cd," +
//      " ac.PNR_Rec_Locr_Id as Rec_Locr_Id,'' as Bkg_Crtn_GTs,'' as SSR_Ind,'' as Tkt_Doc_Nb," +
//      " '' as Tkt_Doc_Iss_Dt ,'' as Comp_Flt_Legs from phx_published_temp1 ac ").distinct().toDF()

//    dfpublished.printSchema()
//    dfpublished.show()

  }



//
//
//    }

}
