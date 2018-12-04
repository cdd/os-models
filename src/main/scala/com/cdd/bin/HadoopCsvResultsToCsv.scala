package com.cdd.bin

import com.cdd.models.utils.Configuration
import com.cdd.spark.utils.DataFrameOps

/**
  * Created by gjones on 6/27/17.
  *
  * Loads in a dataframe from a hadoop CSV results file then saves it to a local single CSV file
  */
object HadoopCsvResultsToCsv extends App {

   if (args.length != 2) {
    println(s"Usage ${getClass.getName} <hadoop csv file> <csv file>")
  } else {
     var hadoopFile = args(0)
     if (!hadoopFile.startsWith("hdfs")) {
       hadoopFile = "hdfs://localhost:9005/os-models/data/pipeline_results/"+hadoopFile
     }
    val csvFile = args(1)

    val df = Configuration.sparkSession.read.option("headers", true).csv(hadoopFile)
    DataFrameOps.dataFrameToCsv(df, csvFile)
  }

}
