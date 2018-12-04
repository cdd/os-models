package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.transformer.FilterSameFeatureValues
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class LinearRegressionSpec extends FunSpec with Matchers{



  describe("Running linear regression model on CDK descriptors") {
    val startDt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")
    // this tests the weighted least squares optimizer - lines 57-76 LinearRegressor.scala
    var dt = startDt.selectAs("no" -> "no", "activity_value" -> "label", "cdk_descriptors" -> "features").filterNull().shuffle(seed=4321L)

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val filter = new FilterSameFeatureValues()
    val elasticNet = new LinearRegressor()
      .setFeaturesColumn("uniqueFeatures")
      .setLambda(0.3)
      .setMaxIter(100)
      .setElasticNetParam(0)
    val pipeline = new Pipeline().setStages(Vector(filter, elasticNet))

    val (testDt, trainDt) = dt.testTrainSplit()

    val lrModel = pipeline.fit(trainDt)
    val predictDt = lrModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.57") {
      rmse should be(0.57 +- 0.1)
    }

    it("should have a test model correlation of about 0.76") {
      c should be(0.76 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.59") {
      dataSplit.getMetric() should be(0.59 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.72") {
      c2 should be(0.72 +- 0.1)
    }

  }

  describe("Running linear regression model on sparse rdkit fingerprints without elastic net") {

    // This evaluates LBFGS optimizer (line 130 LinearRegressor.scala)
    val startDt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")
    var dt = startDt.selectAs("no" -> "no", "activity_value" -> "label", "fingerprints_RDKit_FCFP6" -> "features").filterNull().shuffle(seed=4321L)

    it("should have loaded 13403 molecules") {
      dt.length should be(13403)
    }

    val filter = new FilterSameFeatureValues()

    val elasticNet = new LinearRegressor()
      .setFeaturesColumn("uniqueFeatures")
      .setLambda(0.3)
      .setMaxIter(100)
      .setElasticNetParam(0)
    val pipeline = new Pipeline().setStages(Vector(filter, elasticNet))


    val (testDt, trainDt) = dt.testTrainSplit()
    val lrModel = pipeline.fit(trainDt)
    val predictDt = lrModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.3") {
      rmse should be(0.3 +- 0.1)
    }

    it("should have a test model correlation of about 0.64") {
      c should be(0.64 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.3") {
      dataSplit.getMetric() should be(0.3 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.67") {
      c2 should be(0.67 +- 0.1)
    }

  }

  describe("Running linear regression model on sparse rdkit fingerprints with elastic net") {

    // This evaluates OWLQN optimizeer (line 144 LinearRegressor.scala)
    val startDt = DataTable.loadProjectFile("data/continuous/MalariaGSK.csv.gz")
    var dt = startDt.selectAs("no" -> "no", "activity_value" -> "label", "fingerprints_RDKit_FCFP6" -> "features").filterNull().shuffle(seed=4321L)

    it("should have loaded 13403 molecules") {
      dt.length should be(13403)
    }

    val filter = new FilterSameFeatureValues()

    val elasticNet = new LinearRegressor()
      .setFeaturesColumn("uniqueFeatures")
      .setLambda(0.3)
      .setMaxIter(100)
      .setElasticNetParam(0.5)
    val pipeline = new Pipeline().setStages(Vector(filter, elasticNet))


    val (testDt, trainDt) = dt.testTrainSplit()
    val lrModel = pipeline.fit(trainDt)
    val predictDt = lrModel.transform(testDt)

    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDt)

    val c = evaluator.correlation(predictDt)

    it("should have a test model rmse of about 0.4") {
      rmse should be(0.4 +- 0.1)
    }

    it("should have a test model correlation of about 0.34") {
      c should be(0.34 +- 0.1)
    }

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val c2 = evaluator.correlation(dataSplit.getPredictDt())
    info(s"Correlation is ${c2}")

    it("should have a datasplit model rmse of about 0.4") {
      dataSplit.getMetric() should be(0.4 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.37") {
      c2 should be(0.37 +- 0.1)
    }

  }

}
