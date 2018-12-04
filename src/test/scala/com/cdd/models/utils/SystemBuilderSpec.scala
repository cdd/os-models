package com.cdd.models.utils

import org.scalatest.{FunSpec, Matchers}


/**
  * Created by gjones on 6/7/17.
  */
class SystemBuilderSpec extends FunSpec with Matchers {

  describe("Reading directory properties from a file") {
    var properties: Array[TestSdfileProperties] = null

    Util.using(Util.openPathAsStreamByName("data/continuous/recipe.json")) { in =>
      properties = TestDirectoryLocalProperties.readProperties(in)
    }
  }

  val commonColumns = Set[String]("no", "activity_string", "activity_value", "fingerprints_CDK_ECFP6",
    "fingerprints_CDK_FCFP6", "cdk_smiles", "fingerprints_RDKit_ECFP6", "fingerprints_RDKit_FCFP6", "rdkit_descriptors", "rdkit_smiles", "cdk_descriptors")
  describe("Reading sd files and saving to parquet") {
    val testDirectoryProperties = new TestDirectorySparkProperties(Util.getProjectFilePath("data/continuous"))
    val savedFile = testDirectoryProperties.processFile("Caco2.sdf.gz")
    val df = Configuration.sparkSession.read.parquet(savedFile)
    it("the saved data table should contain the correct columns") {
      df.columns should contain theSameElementsAs commonColumns
    }
    it("should have the correct number of rows") {
      df.count() should be(100)
    }
  }

  describe("Reading sd files with multiple activities and saving to parquet") {
    val testDirectoryProperties = new TestDirectorySparkProperties(Util.getProjectFilePath("data/continuous"))
    val savedFile = testDirectoryProperties.processFile("MouseRatTox.sdf.gz")
    val df = Configuration.sparkSession.read.parquet(savedFile)

    df.printSchema()
    var expectedColumns = commonColumns + ("activity_string_2", "activity_value_2")
    it("the saved data table should contain the correct columns") {
      df.columns should contain theSameElementsAs expectedColumns
    }
    it("should have the correct number of rows") {
      df.count() should be(773)
    }
    it("should have 318 TD50_Mouse values") {
      df.filter("activity_value_2 is not null").count() should be(318)
    }
  }

  describe("Reading sd file with discrete values") {
    val testDirectoryProperties = new TestDirectorySparkProperties(Util.getProjectFilePath("data/discrete"))
    val savedFile = testDirectoryProperties.processFile("data_herg.sdf.gz")
    val df = Configuration.sparkSession.read.parquet(savedFile)

    it("the saved data table should contain the correct columns") {
      df.columns should contain theSameElementsAs commonColumns
    }
    it("should have the correct number of rows") {
      df.count() should be(806)
    }
    it("should have 373 actives") {
      df.filter("activity_value = 1.0").count() should be(373)
    }

  }
}
