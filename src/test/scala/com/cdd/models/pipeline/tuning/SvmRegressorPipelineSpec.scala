package com.cdd.models.pipeline.tuning

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.SvmRegressor
import com.cdd.models.pipeline.transformer.VectorFolder
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import org.scalatest.{FunSpec, Matchers}

class SvmRegressorPipelineSpec extends FunSpec with Matchers{

  describe("Running a SVM classification model on RDKIT fingerprints using a pipeline") {

    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle()

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val (testDt, trainDt) = dt.testTrainSplit()

    val svm = new SvmRegressor().setKernelType("RBF").setGamma(0.01).setFeaturesColumn("foldedFeatures")
    val folder = new VectorFolder().setFoldSize(200)
    val pipeline = new Pipeline().setStages(Vector(folder, svm))
    val svmModel = pipeline.fit(trainDt)
    val predictDt = svmModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.46") {
      rmse should be(0.46 +- 0.1)
    }

    it("should have a test model correlation of about 0.81") {
      c should be(0.81 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.47") {
      dataSplit.getMetric() should be(0.47 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.84") {
      c2 should be(0.84 +- 0.1)
    }

    info("Done")
  }
}
