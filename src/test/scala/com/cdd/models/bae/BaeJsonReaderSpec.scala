package com.cdd.models.bae

import org.scalatest.{FunSpec, Matchers}

class BaeJsonReaderSpec extends FunSpec with Matchers {

  describe("Retrieving assays ids") {
    val assayIds = BaeAssaySummaries.assayIds()

    it("should have 1297 assay ids") {
      assayIds.size should be(1302)
    }
  }

  val uniqueId = "pubchemAID:1077"

  describe("Loading assay and compound information") {


    val baeAssay = new BaeAssay(uniqueId)

    val units = baeAssay.valuesForLabel("units")

    info("hello")
  }

  describe("Reading bae assay compound list") {
    val assayInformation = BaeJsonReader.loadAssayCompoundInformationFromJsonFile(uniqueId)

    info("hello")
  }
}
