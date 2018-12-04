package com.cdd.models.spark.pipeline

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.pipeline.{CdkInputBuilder, DataSplit}
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 5/22/17.
  */
class DataSplitRegressionSpec extends FunSpec with Matchers {

  val sparkSession = Configuration.sparkSession
  var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
  df = DataFrameOps.filterForEstimator(df, "fingerprints_CDK_FCFP6")
  val folder = new VectorFolder().setFoldSize(200)
  df = folder.transform(df)

  describe("Running a model on a dataframe split into test and training with sparse features") {

    it("should have an input dataframe of 743 rows") {
      df.count() should be(743)
    }

    val lr = new GeneralizedLinearRegression().setMaxIter(10).setRegParam(0.3).setLink("log").setFamily("gaussian").setFeaturesCol(folder.getOrDefault(folder.outputCol))
    val evaluator = new RegressionEvaluator()
    val dataSplit = new DataSplit().setEstimator(lr).setEvaluator(evaluator)
    dataSplit.foldAndFit(df)
    val predictDf = dataSplit.getPredictDf()

    it("should have an output dataframe of 743 rows") {
      predictDf.count() should be(743)
    }

    it("should have a prediction column") {
      predictDf.columns.toSet should equal(Set("label", "features", "prediction", "no", "foldedFeatures"))
    }

    it("should have a model rmse of about 0.55") {
      dataSplit.getMetric() should be(0.55 +- 0.1)
    }

    it("should have a model correlation of about 0.75") {
      dataSplit.getCorrelation() should be(0.75 +- 0.1)
    }
    info(s"Metric is ${dataSplit.getMetric()}")
    info(s"Correlation is ${dataSplit.getCorrelation()}")

  }
}
