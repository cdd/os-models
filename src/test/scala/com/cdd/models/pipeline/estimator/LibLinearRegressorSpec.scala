package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.VectorFolder
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class LibLinearRegressorSpec extends FunSpec with Matchers {

  describe("Running a Lib Linear SVM regression model on RDKIT fingerprints") {

    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle()

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val folder = new VectorFolder().setFoldSize(200)
    dt = folder.transform(dt)
    val (testDt, trainDt) = dt.testTrainSplit()

    val svm = new LibLinearRegressor()
      .setFeaturesColumn("foldedFeatures")
      .setEps(0.001)
      .setRegularizationType("L2")
      .setMaxIterations(100)
      .setC(0.1)
    val svmModel = svm.fit(trainDt)

    val predictDt = svmModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.57") {
      rmse should be(0.57 +- 0.1)
    }

    it("should have a test model correlation of about 0.72") {
      c should be(0.72 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(svm)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.62") {
      dataSplit.getMetric() should be(0.62 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.76") {
      c2 should be(0.76 +- 0.1)
    }

  }
}
