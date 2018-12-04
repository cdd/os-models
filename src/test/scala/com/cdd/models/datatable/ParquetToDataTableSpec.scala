package com.cdd.models.datatable

import com.cdd.models.utils.{Configuration, Util}
import org.scalatest.{FunSpec, Matchers}

class ParquetToDataTableSpec extends FunSpec with Matchers {

  describe("Reading a parquet file and saving to to CSV") {
    val parquetFile = Util.getProjectFilePath("data/continuous/MalariaGSK.parquet").getAbsolutePath
    val df = Configuration.sparkSession.read.parquet(parquetFile)
    val dt = DataTable.fromSpark(df, true)

    it ("should be of size 13519") {
      df.count() should be(13519)
      dt.length should be(13519)
    }

    it ("should have 13403 activity values") {
      df.select("activity_value").filter("activity_value is not null").count() should be(13403)
      dt.select("activity_value").filterNull().length should be(13403)
    }

    val csvFile = "/tmp/MalariaGSK.csv.gz"
    dt.exportToFile(csvFile)
    val dt2 = DataTable.loadFile(csvFile)

     it ("should be of size 13519 after save and reload") {
      dt2.length should be(13519)
    }

    it ("should have 13403 activity values after save and reload") {
      dt2.select("activity_value").filterNull().length should be(13403)
    }

    info("done")
  }
}
