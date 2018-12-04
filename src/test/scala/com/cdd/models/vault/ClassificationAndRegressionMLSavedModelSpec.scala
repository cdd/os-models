package com.cdd.models.vault

import com.cdd.models.pipeline.{ClassificationEvaluator, PipelineModel}
import com.cdd.models.pipeline.estimator.MajorityVotingClassificationModel
import org.scalatest.{FunSpec, Matchers}

class ClassificationAndRegressionMLSavedModelSpec  extends FunSpec with  Matchers {
  describe("Saving ML models to disk and checking reproducibility of results") {

    val models = ClassificationAndRegressionML.buildAndSaveModelsOnAllData()
    it("should create 5 models") {
      models.size should be(5)
    }

    val votingModel = models(4).asInstanceOf[PipelineModel]
    it("should have a majority voting model") {
      votingModel.pipelineModels.size should be(1)
      votingModel.pipelineModels(0).isInstanceOf[MajorityVotingClassificationModel] should be(true)
    }

    val dataTableIn = ClassificationAndRegressionML.buildDataTable(true)
    val evaluator = new ClassificationEvaluator()
    val metric = 0.941

   describe("Transforming results on in-memory model") {
      val dataTable = votingModel.transform(dataTableIn)
      val metric = evaluator.evaluate(dataTable)

      it ("should have the right accuracy") {
        metric should be(metric)
      }
    }


   describe("Transforming results on saved model") {
     val modelFileName = ClassificationAndRegressionML.savedModelFileName("Voting Classifier")
     val savedVotingModel = PipelineModel.loadPipeline(modelFileName)
      val dataTable = savedVotingModel.transform(dataTableIn)
      val metric = evaluator.evaluate(dataTable)

      it ("should have the right accuracy") {
        metric should be(metric)
      }
    }
  }
}
