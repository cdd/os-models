package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.scalatest.{FunSpec, Matchers}

class LinearRegressionOnCdkDescriptorsSpec extends FunSpec with Matchers{

  val sparkSession = Configuration.sparkSession
  var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
  df = DataFrameOps.filterForEstimator(df, "cdk_descriptors")

  it("should have loaded 741 molecules") {
    df.count() should be(741)
  }

  var filter = new FilterSameFeatureValues().setInputCol("features").setOutputCol("filteredFeatures")
  val scaler = new StandardScaler()
    .setInputCol("filteredFeatures")
    .setOutputCol("normFeatures")
    .setWithMean(true).setWithStd(true)
  val lr = new LinearRegression().setMaxIter(10).setRegParam(0.1).setFeaturesCol("normFeatures")
  val pipeline = new Pipeline().setStages(Array(filter, scaler, lr))

  val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed=1234L)
  val evaluator = new RegressionEvaluator()

  val lrModel = pipeline.fit(training)

  var predictDf = lrModel.transform(test)
  val rmse = evaluator.evaluate(predictDf)

  info(s"rmse is $rmse")

  val c = predictDf.stat.corr("label", "prediction")
  info(s"correlation coefficient $c")

  it("should have a test model rmse of about 0.56") {
    rmse should be(0.56 +- 0.1)
  }

  it("should have a test model correlation of about 0.82") {
    c should be(0.80 +- 0.1)
  }
}
