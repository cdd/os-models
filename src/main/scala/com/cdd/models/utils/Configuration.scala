package com.cdd.models.utils

import com.amazonaws.regions.Regions
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by gareth on 5/16/17.
  */
object Configuration extends  HasLogging {
  private var session: Option[SparkSession] = None

  var additionalSparkSettings: Option[Map[String, String]] = None
  def sparkSession: SparkSession = buildSession()
  def sparkContext: SparkContext = buildSession().sparkContext

  def buildSession(): SparkSession =  {
    if (session != None)
      return session.get

    // spark session, local with 2 CPUs
    //val sparkSession = SparkSession.builder().master("local[4]").appName("CDD Models").getOrCreate()
    val conf = new SparkConf()
    if (additionalSparkSettings != None) {
      additionalSparkSettings.get.foreach { case(k, v) => conf.set(k, v)}
    }
    conf.getAll.foreach { case(k, v) => logger.warn(s"Config $k -> $v") }
    val builder =  SparkSession.builder().appName("CDD Models")
    if (!conf.contains("spark.master" )) {
      builder.master("local[8]")
    }
    logger.warn("Creating spark session")
    val sparkSession = builder.appName("CDD Models").config(conf).getOrCreate()
    session = Some(sparkSession)
    sparkSession
  }

  /**
    * Return true if the code is running in an AWS instance
    * @return
    */
  def runningInAws(): Boolean = {
    // see https://aws.amazon.com/blogs/developer/determining-an-applications-current-region/
    // if you're not running on AWS the region will be null
    Regions.getCurrentRegion() != null
  }

}

