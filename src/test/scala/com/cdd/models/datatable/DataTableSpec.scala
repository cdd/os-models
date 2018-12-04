package com.cdd.models.datatable

import com.cdd.models.utils._
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 7/25/17.
  */
class DataTableSpec extends FunSpec with Matchers {

  describe("Reading a Parquet file using spark and loading as a data table") {
    val df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    it("should have loaded a spark dataframe of 742 columns") {
      df.count() should be(743)
    }
    val dt = DataTable.fromSpark(df, true)
    info("loaded data table")
    it("should have created a data table of 742 columns") {
      dt.length should be(743)
    }

    describe("Saving the data table as CSV") {
      val csvFile = Util.getProjectFilePath("data/continuous/Chagas.csv.gz").getAbsolutePath
      dt.exportToFile(csvFile)
      info("exported data table")
    }
  }

  describe("Loading a CSV file into a data table") {
    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    it("should have imported a data table of 742 columns") {
      dt.length should be(743)
    }
    info("imported data table")
  }

  describe("Reading a SDF file to a data table") {
    val file = Util.getProjectFilePath("data/continuous/Chagas.sdf.gz").getAbsolutePath
    val property = SdfileActivityField("IC50_uM", ActivityCategory.CONTINUOUS, None)
    val dt = SdfileReader.sdFileToDataTable(file, Vector(property))

    info("loaded data table")
    it("should have created a data table of 742 columns") {
      dt.length should be(743)
    }
  }
}
