package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, MaxMinScaler, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class EpochNeuralNetworkClassifierSpec extends FunSpec with Matchers {

  describe("Running a Epoch neural network classification model on RDKIT fingerprints") {

    var dt = DataTable.loadProjectFile("data/discrete/data_chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)

    it("should have loaded 4055 molecules") {
      dt.length should be(4055)
    }

    val filter = new FilterSameFeatureValues()
    val folder = new VectorFolder().setFoldSize(500).setFeaturesColumn("uniqueFeatures")
    val scaler = new MaxMinScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("scaledFeatures")
    dt = filter.fit(dt).transform(dt)
    dt = folder.transform(dt)
    dt = scaler.fit(dt).transform(dt)

    val (testDt, trainDt) = dt.testTrainSplit()

    val nnc = new EpochNeuralNetworkClassifier()
      .setFeaturesColumn("scaledFeatures")
      .setNoEpochs(100)
      .setLamdba(0.01)
      .setLayers(Vector(75))
    val nncModel = nnc.fit(trainDt)

    val predictDt = nncModel.transform(testDt)

    val evaluator = new ClassificationEvaluator().setMetric("accuracy")
    val accuracy = evaluator.evaluate(predictDt)

    val auc = evaluator.rocAuc(predictDt)
    it("should have a test model accuracy of about 0.69") {
      accuracy should be(0.69 +- 0.1)
    }

    it("should have a test model rocAuc of about 0.69") {
      auc should be(0.69 +- 0.1)
    }

    val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(nnc)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
    info(s"AUC is ${auc2}")

    it("should have a datasplit model accuracy of about 0.71") {
      dataSplit.getMetric() should be(0.71 +- 0.1)
    }

    it("should have a datasplit model AUC of about 0.74") {
      auc2 should be(0.74 +- 0.1)
    }

    info("Done")
  }
}
