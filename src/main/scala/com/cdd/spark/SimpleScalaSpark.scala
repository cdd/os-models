package com.cdd.spark

import com.cdd.models.utils.Util
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gjones on 5/10/17.
  */
object SimpleScalaSpark {

  def main(args: Array[String]) {
    println(s"classpath home is ${Util.getClassPath()}")
    val logFile = "/Users/gareth/unison.log" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}