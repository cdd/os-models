package com.cdd.models.pipeline.tuning

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.estimator.SvmRegressor
import com.cdd.models.pipeline.transformer.{StandardScaler, FilterSameFeatureValues, VectorFolder}
import org.scalatest.{FunSpec, Matchers}

class SvmRegressorNormalizerSpec extends FunSpec with Matchers {
  describe("Running a SVM classification model on RDKIT descriptors with normalization using a pipeline") {

    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("rdkit_descriptors" -> "features")).filterNull().shuffle()

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val (testDt, trainDt) = dt.testTrainSplit()

    val svm = new SvmRegressor().setKernelType("RBF").setGamma(0.01).setFeaturesColumn("normalizedFeatures")
    val filter = new FilterSameFeatureValues().setFeaturesColumn("features").setOutputColumn("uniqueFeatures")
    val normalizer = new StandardScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
    val pipeline = new Pipeline().setStages(Vector(filter, normalizer, svm))
    val svmModel = pipeline.fit(trainDt)
    val predictDt = svmModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.53") {
      rmse should be(0.53 +- 0.1)
    }

    it("should have a test model correlation of about 0.75") {
      c should be(0.75 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.53") {
      dataSplit.getMetric() should be(0.53 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.79") {
      c2 should be(0.79 +- 0.1)
    }

  }
}
