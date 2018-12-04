package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, MaxMinScaler, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SmileRandomForestClassifierSpec extends FunSpec with Matchers {

  describe("Running a Smile random forest classification model on RDKIT fingerprints") {

    var dt = DataTable.loadProjectFile("data/discrete/data_chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)

    it("should have loaded 4055 molecules") {
      dt.length should be(4055)
    }

    val filter = new FilterSameFeatureValues()
    val folder = new VectorFolder().setFoldSize(500).setFeaturesColumn("uniqueFeatures")
    dt = filter.fit(dt).transform(dt)
    dt = folder.transform(dt)

    val (testDt, trainDt) = dt.testTrainSplit()

    val rf = new SmileRandomForestClassifier()
      .setFeaturesColumn("foldedFeatures")
      .setNumTrees(10)
      .setMaxNodes(1000)
    val rfModel = rf.fit(trainDt)

    val predictDt = rfModel.transform(testDt)

    val evaluator = new ClassificationEvaluator().setMetric("accuracy")
    val accuracy = evaluator.evaluate(predictDt)

    val auc = evaluator.rocAuc(predictDt)
    it("should have a test model accuracy of about 0.69") {
      accuracy should be(0.69 +- 0.1)
    }

    it("should have a test model rocAuc of about 0.76") {
      auc should be(0.76 +- 0.1)
    }

    val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(rf)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
    info(s"AUC is ${auc2}")

    it("should have a datasplit model accuracy of about 0.72") {
      dataSplit.getMetric() should be(0.72 +- 0.1)
    }

    it("should have a datasplit model AUC of about 0.77") {
      auc2 should be(0.77 +- 0.1)
    }

    info("Done")
  }
}
