package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.DataSplit
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/6/17.
  */
class SimpleRegressionOnRdkitDescriptorsSpec extends FunSpec with Matchers {

  describe("Running a classification model on RDKIT properties") {

    val sparkSession = Configuration.sparkSession
    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    df = DataFrameOps.filterForEstimator(df, "rdkit_descriptors")

    it("should have loaded 741 molecules") {
      df.count() should be(741)
    }

    var filter = new FilterSameFeatureValues().setInputCol("features").setOutputCol("filteredFeatures")
    val scaler = new StandardScaler()
      .setInputCol("filteredFeatures")
      .setOutputCol("normFeatures")
      .setWithMean(true).setWithStd(true)
    val lr = new GeneralizedLinearRegression().setMaxIter(10).setRegParam(0.1).setFamily("gaussian").setFeaturesCol("normFeatures")
    val pipeline = new Pipeline().setStages(Array(filter, scaler, lr))

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed=1234L)
    val evaluator = new RegressionEvaluator()

    val pipelineModel = pipeline.fit(training)
    var predictDf = pipelineModel.transform(test)
    val rmse = evaluator.evaluate(predictDf)

    info(s"rmse is $rmse")

    val c = predictDf.stat.corr("label", "prediction")
    info(s"correlation coefficient $c")


    it("should have a test model rmse of about 0.54") {
      rmse should be(0.54 +- 0.1)
    }

    it("should have a test model correlation of about 0.77") {
      c should be(0.77 +- 0.1)
    }

    val dataSplit = new DataSplit().setEstimator(pipeline).setEvaluator(evaluator)
    dataSplit.foldAndFit(df)

    info(s"Metric is ${dataSplit.getMetric()}")
    info(s"Correlation is ${dataSplit.getCorrelation()}")

    it("should have a datasplit model rmse of about 0.62") {
      dataSplit.getMetric() should be(0.62 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.69") {
      dataSplit.getCorrelation() should be(0.69 +- 0.1)
    }
  }

}
