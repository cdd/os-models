package com.cdd.models.datatable

import com.cdd.models.utils.HasLogging

object ParallelDataTable extends HasLogging {

  def process(dataTable: DataTable, fn:DataTable => DataTable, chunkSize:Int): DataTable = {
    val inputTables = dataTable.split(chunkSize)
    val outputTables = inputTables.par.map( dt => fn(dt))//.toVector
    val dt = outputTables.reduceLeft { (a, b) => a.append(b) }
    assert(dt.length == dataTable.length)
    dt
  }


}
