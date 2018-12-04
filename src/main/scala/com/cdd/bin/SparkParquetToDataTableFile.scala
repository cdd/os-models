package com.cdd.bin

import java.io.File

import com.cdd.models.datatable.DataTable
import com.cdd.models.utils.{Configuration, Util}

object SparkParquetToDataTableFile extends App {

  if (args.length != 2) {
    println(s"Usage ${getClass.getName} <partitioned parquet file> <data table file>")
  } else {
    val parquetFile = args(0)
    val csvFile = args(1)

    if (new File(csvFile).exists()) {
      throw new IllegalArgumentException(s"file $csvFile exists!")
    }

    val df = Configuration.sparkSession.read.parquet(parquetFile)
    val dt = DataTable.fromSpark(df, true)
    dt.exportToFile(csvFile)
  }
}
