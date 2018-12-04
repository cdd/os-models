package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SmileSvmRegressorSpec extends FunSpec with Matchers {
  describe("Running a Smile toolkit regression on RDKIT fingerprints") {
    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    //var dt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")
    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)
    val filter = new FilterSameFeatureValues()
    val dataTable = filter.fit(dt).transform(dt)

    it("should have loaded 741 molecules") {
      dataTable.length should be(741)
    }

    describe("Performing regression using raw (sparse) features and a Tanimoto kernel") {
      val dt = dataTable
      val (testDt, trainDt) = dt.testTrainSplit()

      val svr = new SmileSvmRegressor()
        .setFeaturesColumn("uniqueFeatures")
        .setKernelType("Tanimoto")
        .setGamma(1.0)
        .setCoef0(1.0)
        .setDegree(3)
        .setC(1.0)
      val svrModel = svr.fit(trainDt)

      val predictDt = svrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.45") {
        rmse should be(0.45 +- 0.1)
      }

      it("should have a test model correlation of about 0.86") {
        c should be(0.86 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(svr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.43") {
        dataSplit.getMetric() should be(0.43 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.87") {
        c2 should be(0.87 +- 0.1)
      }
    }

    describe("Performing regression using folded (dense) features and a RBF kernel") {
      val folder = new VectorFolder().setFoldSize(200).setFeaturesColumn("uniqueFeatures")
      var dt = folder.transform(dataTable)
      val (testDt, trainDt) = dt.testTrainSplit()

      val svr = new SmileSvmRegressor()
        .setFeaturesColumn("foldedFeatures").setKernelType("RBF")
        .setGamma(0.01)
        .setC(1.0)
      val svrModel = svr.fit(trainDt)

      val predictDt = svrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.47") {
        rmse should be(0.47 +- 0.1)
      }

      it("should have a test model correlation of about 0.85") {
        c should be(0.85 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(svr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.46") {
        dataSplit.getMetric() should be(0.46 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.85") {
        c2 should be(0.85 +- 0.1)
      }
    }

    describe("Performing regression using raw (sparse) features and a RBF kernel") {
      val dt = dataTable
      val (testDt, trainDt) = dt.testTrainSplit()


      val svr = new SmileSvmRegressor()
        .setFeaturesColumn("uniqueFeatures").setKernelType("RBF")
        .setGamma(0.01)
        .setC(1.0)
      val svrModel = svr.fit(trainDt)

      val predictDt = svrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.46") {
        rmse should be(0.46 +- 0.1)
      }

      it("should have a test model correlation of about 0.86") {
        c should be(0.86 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(svr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.45") {
        dataSplit.getMetric() should be(0.45 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.85") {
        c2 should be(0.85 +- 0.1)
      }
    }
  }
}


