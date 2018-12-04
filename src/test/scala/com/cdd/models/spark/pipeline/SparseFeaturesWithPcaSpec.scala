package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.scalatest.{FunSpec, Ignore, Matchers}

/**
  * Created by gjones on 5/20/17.
  *
  * Turned off this test as it takes almost 5 min
  */
@Ignore
class SparseFeaturesWithPcaSpec extends FunSpec with Matchers {

  describe("Running a model on a dataframe split into test and training with sparse features") {


    val sparkSession = Configuration.sparkSession
    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    df = DataFrameOps.filterForEstimator(df, "fingerprints_RDKit_FCFP6")
    val folder = new VectorFolder().setFoldSize(5000)
    df = folder.transform(df)

    val lr = new GeneralizedLinearRegression().setMaxIter(10).setRegParam(0.3).setLink("log").setFamily("gaussian").setFeaturesCol("pcaFeatures")

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2))

    // val pca = new PCA().setInputCol(folder.get(folder.outputCol).get).setOutputCol("pcaFeatures").setK(100).fit(training)
    val pca = new PCA().setInputCol(folder.getOrDefault(folder.outputCol)).setOutputCol("pcaFeatures").setK(100).fit(training)

    val pcaTraining = pca.transform(training)
    val pcaTest = pca.transform(test)


    val lrModel = lr.fit(pcaTraining)
    info(s"Model summary ${lrModel.summary}")
    //val trainValidationSplit = new TrainValidationSplit().setEstimator(lr).setEvaluator(evaluator).setTrainRatio(.8)

    val predictDf = lrModel.transform(pcaTest)
    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDf)

    info(s"rmse is $rmse")

    val c = predictDf.stat.corr("label", "prediction")
    info(s"correlation coefficient $c")

    info("done split")

    it("should have a model rmse of about 0.50") {
      rmse should be(0.55 +- 0.1)
    }

    it("should have a model correlation of about 0.86") {
      c should be(0.76 +- 0.1)
    }
    //val mat: RowMatrix
    //mat.computeSVD()
  }
}
