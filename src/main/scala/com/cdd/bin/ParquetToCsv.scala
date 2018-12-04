package com.cdd.bin

import com.cdd.models.utils.Configuration
import com.cdd.spark.utils.DataFrameOps

/**
  * Created by gjones on 7/7/17.
  */
object ParquetToCsv extends App {

  if (args.length != 2) {
    println(s"Usage ${getClass.getName} <partitioned parquet file> <csv file>")
  } else {
    val parquetFile = args(0)
    val csvFile = args(1)

    val df = Configuration.sparkSession.read.option("headers", true).parquet(parquetFile)
    DataFrameOps.dataFrameToCsv(df, csvFile)
  }
}
