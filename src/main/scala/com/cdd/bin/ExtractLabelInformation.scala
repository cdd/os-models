package com.cdd.bin

import com.cdd.models.utils.Configuration
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.sql.functions._


/**
  * Created by gjones on 6/26/17.
  *
  * Given a Parquet file, dumps label information to a CSV file
  */
object ExtractLabelInformation extends App {

  if (args.length != 2) {
    println(s"Usage ${getClass.getName} <parquet data> <csv file>")
  } else {
    val parquetFile = args(0)
    val csvFile = args(1)
    val df = Configuration.sparkSession.read.parquet(parquetFile)
    val activityColumns = df.columns.filter {
      _.contains("activity")
    }.toList
    val labelDf = df.select(activityColumns.map {
      col(_)
    }: _*)
    DataFrameOps.dataFrameToCsv(labelDf, csvFile)
  }
}
