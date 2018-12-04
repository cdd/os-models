package com.cdd.models.tox

import com.cdd.models.tox.PredictOnReference.StructureReferenceSet
import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.webservices.{Tox21ActivityRequest, Tox21PostClient}
import org.scalatest.{FunSpec, Matchers}

/**
  * This test requires the Jetty tox server be running
  *
  * The test will fail if the server runs for any length of time as random number effects kick in
  */
class Tox21TesterSpec extends FunSpec with Matchers with HasLogging {

  val toxFields = Tox21ClassificationModel.toxAssays.slice(0, 2)

  describe("Testing CMC compounds against Tox21 Models and comparing to service results") {

    logger.info("Submitting server request")
    val smiles = PredictOnReference.referenceSmiles(StructureReferenceSet.CMC)
    val (results, _) = Util.time {
      Tox21PostClient.testSmiles(smiles.toArray, "consensus") //targets = Array("NR-AR", "NR-AhR"))
    }

    val result1 = Tox21Tester.compoundActivityHitrate(toxFields(0), results)
    logger.info(s"N tested ${result1.nTotal} N predicted ${result1.nHits} hitrate ${result1.hitRate}")
    val result2 = Tox21Tester.compoundActivityHitrate(toxFields(1), results)
    logger.info(s"N tested ${result2.nTotal} N predicted ${result2.nHits} hitrate ${result2.hitRate}")

    it(s"should have 291 hits for target ${toxFields(0)} for service") {
      result1.nHits should be(291)
      result1.nTotal should be(4608)
    }

    it(s"should have 519 hits for target ${toxFields(1)} for service") {
      result2.nHits should be(519)
      result2.nTotal should be(4608)
    }

    logger.info("Predicting directly")
    val dataTable = PredictOnReference.dataTableForRef(StructureReferenceSet.CMC)
    val cmcResults = toxFields.map { toxField =>
      val results = Tox21Tester.testDataTable(toxField, Tox21TesterModel.Consensus, dataTable)
      results
    }

    it(s"should have 291 hits for target ${toxFields(0)} on reference") {
      cmcResults(0).nPredicted should be(291)
      cmcResults(0).predictTable.length should be(4608)
    }

    it(s"should have 519 hits for target ${toxFields(1)} on reference") {
      cmcResults(1).nPredicted should be(519)
      cmcResults(1).predictTable.length should be(4608)
    }

    logger.info("Comparing results")
    it ("should have the same hits for reference and service") {
      cmcResults(0).compoundNos.sorted should be(result1.compounds.sorted)
      cmcResults(1).compoundNos.sorted should be(result2.compounds.sorted)
    }

  }

}
