package com.cdd.webservices

import com.cdd.models.tox.PredictOnReference.StructureReferenceSet
import com.cdd.models.tox.Tox21Tester.CompoundActivitySummary
import com.cdd.models.tox.{PredictOnReference, Tox21TesterModel}
import com.cdd.models.utils.{HasLogging, Util}
import com.google.gson.Gson

object Tox21PostClient extends HasLogging {
  private val gson = new Gson()

  def main(args: Array[String]): Unit = {
    val smiles = if (args.isEmpty) {
      Array("This is not a smiles", "c1ccccc1O",
        "CC(=O)O[C@H]1C[C@H](O[C@H]2[C@@H](O)C[C@H](O[C@H]3[C@@H](O)C[C@H](O[C@H]4CC[C@]5(C)[C@H]6CC[C@]7(C)[C@@H](C8=CC(=O)OC8)CC[C@]7(O)[C@@H]6CC[C@@H]5C4)O[C@@H]3C)O[C@@H]2C)O[C@H](C)[C@H]1O")
    } else {
      args
    }

    testSmiles(smiles, "Consensus")
    testSmiles(smiles, "TunedConsensus")
    testSmiles(smiles, "ConsensusIncludingScoring")
    testSmiles(smiles, "TunedConsensusIncludingScoring")
  }

  def testSmiles(smiles: Array[String], model: String = "consensus", targets: Array[String] = Array.empty,
                 readTimeout: Int = 1200000): Array[CompoundActivitySummary] = {
    Tox21TesterModel.withName(model)
    val activityRequest = new Tox21ActivityRequest(model, smiles, targets)
    val inputJson = gson.toJson(activityRequest)
    val url = "http://localhost:8200/tox21Activity"
    logger.info(s"sending to $url request $inputJson")
    val (results, content) = Util.postJsonObjectToUrl[Tox21ActivityRequest, Array[CompoundActivitySummary]](url, activityRequest,
      readTimeout = readTimeout)
    logger.info(s"got response $content")

    results
  }
}

object LoadTox21Server extends App with HasLogging {

  val smiles = PredictOnReference.referenceSmiles(StructureReferenceSet.NCI)
  smiles.grouped(5000).zipWithIndex.foreach { case(chunk, no) =>
    logger.info(s"Processing chunk $no")
      val (results, time) = Util.time {
      Tox21PostClient.testSmiles(chunk.toArray, "BestConsensus")
    }
    logger.info(s"Processed chunk $no of ${chunk.length} smiles in ${time/1000} seconds")
  }
}
