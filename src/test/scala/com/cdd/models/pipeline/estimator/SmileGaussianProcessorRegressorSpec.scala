package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SmileGaussianProcessorRegressorSpec extends FunSpec with Matchers {
  describe("Running a Smile toolkit Gaussian process regression model on RDKIT fingerprints") {
    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    //var dt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")
    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)
    val filter = new FilterSameFeatureValues()
    val dataTable = filter.fit(dt).transform(dt)

    it("should have loaded 741 molecules") {
      dataTable.length should be(741)
    }

    describe("Performing regression using folded (dense) features") {
      val folder = new VectorFolder().setFoldSize(1000).setFeaturesColumn("uniqueFeatures")
      var dt = folder.transform(dataTable)
      val (testDt, trainDt) = dt.testTrainSplit()

      val gpr = new SmileGaussianProcessRegressor()
        .setFeaturesColumn("foldedFeatures").setKernelType("RBF")
        .setGamma(0.01)
        .setLambda(0.1)
      val rfrModel = gpr.fit(trainDt)

      val predictDt = rfrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.52") {
        rmse should be(0.52 +- 0.1)
      }

      it("should have a test model correlation of about 0.82") {
        c should be(0.82 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(gpr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.57") {
        dataSplit.getMetric() should be(0.57 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.79") {
        c2 should be(0.79 +- 0.1)
      }
    }


    describe("Performing regression using raw (sparse) features") {
      val dt = dataTable
      val (testDt, trainDt) = dt.testTrainSplit()

      val gpr = new SmileGaussianProcessRegressor()
        .setFeaturesColumn("uniqueFeatures").setKernelType("RBF")
        .setGamma(0.01)
        .setLambda(0.1)
      val rfrModel = gpr.fit(trainDt)

      val predictDt = rfrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.51") {
        rmse should be(0.51 +- 0.1)
      }

      it("should have a test model correlation of about 0.81") {
        c should be(0.82 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(gpr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.56") {
        dataSplit.getMetric() should be(0.56 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.79") {
        c2 should be(0.79 +- 0.1)
      }
    }
  }
}
