package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.FilterSameFeatureValues
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SvmRegressorTanimotoSpec extends FunSpec with Matchers {

  describe("Running a SVM regression model on RDKIT fingerprints with Tanimoto Kernel") {

    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle()

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val filter = new FilterSameFeatureValues()
    dt = filter.fit(dt).transform(dt)
    val (testDt, trainDt) = dt.testTrainSplit()

    val svm = new SvmRegressor().setKernelType("TANIMOTO").setGamma(1).setCoef0(1).setDegree(3).setFeaturesColumn("uniqueFeatures")
    val svmModel = svm.fit(trainDt)

    val predictDt = svmModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.43") {
      rmse should be(0.43 +- 0.1)
    }

    it("should have a test model correlation of about 0.84") {
      c should be(0.84 +- 0.1)
    }

    evaluator.plot("/tmp/svm_tan_regression_tts.png", predictDt)
    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(svm).setNumFolds(5)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.43") {
      dataSplit.getMetric() should be(0.43 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.86") {
      c2 should be(0.86 +- 0.1)
    }

    evaluator.plot("/tmp/svm_tan_regression_ds.png", dataSplit.getPredictDt(), "Chagas SVR with Tanimoto kernel")

    info("Done")
  }
}
