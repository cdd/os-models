package com.cdd.models.spark.pipeline.estimator

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.DataSplit
import com.cdd.spark.pipeline.estimator.LibSvmRegressionEstimator
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/13/17.
  */
class LibSvmRegressionEstimatorSpec extends FunSpec with Matchers {

  describe("Running a SVR regression model on RDKIT fingerprints") {

    val sparkSession = Configuration.sparkSession
    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    df = DataFrameOps.filterForEstimator(df, "fingerprints_RDKit_ECFP6").coalesce(1)
    df = new VectorFolder().setFoldSize(200).transform(df)

    it("should have loaded 741 molecules") {
      df.count() should be(741)
    }

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val svmr = new LibSvmRegressionEstimator().setKernelType("RBF").setGamma(0.01).setFeaturesCol("foldedFeatures")
    val evaluator = new RegressionEvaluator()

    val svmModel = svmr.fit(training)

    var predictDf = svmModel.transform(test)
    val rmse = evaluator.evaluate(predictDf)

    info(s"rmse is $rmse")

    val c = predictDf.stat.corr("label", "prediction")
    info(s"correlation coefficient $c")


    it("should have a test model rmse of about 0.48") {
      rmse should be(0.48 +- 0.1)
    }

    it("should have a test model correlation of about 0.86") {
      c should be(0.86 +- 0.1)
    }

    val dataSplit = new DataSplit().setEstimator(svmr).setEvaluator(evaluator)
    dataSplit.foldAndFit(df)

    info(s"Metric is ${dataSplit.getMetric()}")
    info(s"Correlation is ${dataSplit.getCorrelation()}")

    it("should have a datasplit model rmse of about 0.48") {
      dataSplit.getMetric() should be(0.48 +- 0.1)
    }

    it("should have a datasplit model correlation of about 0.83") {
      dataSplit.getCorrelation() should be(0.83 +- 0.1)
    }
  }


}
