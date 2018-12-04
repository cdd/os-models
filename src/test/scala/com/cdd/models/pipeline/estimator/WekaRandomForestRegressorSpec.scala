package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.{BinarizeTransformer, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class WekaRandomForestRegressorSpec extends FunSpec with Matchers {

  describe("Running Weka Random Forest regression model on RDKIT fingerprints") {
    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    //var dt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")
    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)
    val filter = new FilterSameFeatureValues()
    val dataTable = filter.fit(dt).transform(dt)

    it("should have loaded 741 molecules") {
      dataTable.length should be(741)
    }

    /*
    it("should have loaded 13403 molecules") {
      dt.length should be(13403)
    }
    */

    describe("Performing regression using folded (dense) features") {
      val folder = new VectorFolder().setFoldSize(1000).setFeaturesColumn("uniqueFeatures")
      var dt = folder.transform(dataTable)
      val (testDt, trainDt) = dt.testTrainSplit()

      val rfr = new WekaRandomForestRegressor()
        .setFeaturesColumn("foldedFeatures")
        .setNumTrees(60)
        .setMaxDepth(30)
        .setNominalFeaturesCutoff(-1)
      val rfrModel = rfr.fit(trainDt)

      val predictDt = rfrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.52") {
        rmse should be(0.52 +- 0.1)
      }

      it("should have a test model correlation of about 0.81") {
        c should be(0.81 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(rfr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.50") {
        dataSplit.getMetric() should be(0.50 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.82") {
        c2 should be(0.82 +- 0.1)
      }
    }


    describe("Performing regression using raw (sparse) features") {
      //val dt = new BinarizeTransformer().setFeaturesColumn("uniqueFeatures")transform(dataTable)
      val dt = dataTable
      val (testDt, trainDt) = dt.testTrainSplit()

      val rfr = new WekaRandomForestRegressor()
        .setFeaturesColumn("uniqueFeatures")
        .setNumTrees(60)
        .setMaxDepth(30)
        .setNominalFeaturesCutoff(-1)
      //  .setFeaturesColumn("binaryFeatures")
      val rfrModel = rfr.fit(trainDt)

      val predictDt = rfrModel.transform(testDt)

      val evaluator = new RegressionEvaluator()
      val rmse = evaluator.evaluate(predictDt)

      val c = evaluator.correlation(predictDt)

      it("should have a test model rmse of about 0.54") {
        rmse should be(0.54 +- 0.1)
      }

      it("should have a test model correlation of about 0.81") {
        c should be(0.81 +- 0.1)
      }
      val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(rfr)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val c2 = evaluator.correlation(dataSplit.getPredictDt())
      info(s"Correlation is ${c2}")

      it("should have a datasplit model rmse of about 0.52") {
        dataSplit.getMetric() should be(0.52 +- 0.1)
      }

      it("should have a datasplit model correlation of about 0.82") {
        c2 should be(0.82 +- 0.1)
      }
    }
  }
}