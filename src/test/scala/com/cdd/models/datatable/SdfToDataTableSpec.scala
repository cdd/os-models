package com.cdd.models.datatable

import com.cdd.models.utils.{ActivityCategory, SdfileActivityField, Util}
import org.scalatest.{FunSpec, Matchers}

class SdfToDataTableSpec extends FunSpec with Matchers {
  val sdFile = Util.getProjectFilePath("data/continuous/MalariaGSK.sdf.gz").getAbsolutePath
  val property = SdfileActivityField("Activity", ActivityCategory.CONTINUOUS, None)
  val dt = SdfileReader.sdFileToDataTable(sdFile, Vector(property))

  it("should be of size 13519") {
    dt.length should be(13519)
  }

  it("should have 13403 activity values") {
    dt.select("activity_value").filterNull().length should be(13403)
  }

  val csvFile = "/tmp/MalariaGSK.csv.gz"
  dt.exportToFile(csvFile)
  val dt2 = DataTable.loadFile(csvFile)

  it("should be of size 13519 after save and reload") {
    dt2.length should be(13519)
  }

  it("should have 13403 activity values after save and reload") {
    dt2.select("activity_value").filterNull().length should be(13403)
  }

  info("done")
}
