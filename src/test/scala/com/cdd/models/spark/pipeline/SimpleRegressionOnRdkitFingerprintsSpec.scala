package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/6/17.
  */
class SimpleRegressionOnRdkitFingerprintsSpec extends FunSpec with Matchers {

  describe("Performing a model fitting on DeMorgan fingerprints") {


    val sparkSession = Configuration.sparkSession
    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    df = DataFrameOps.filterForEstimator(df, "fingerprints_RDKit_FCFP6")
    val filter = new FilterSameFeatureValues()
    val folder = new VectorFolder().setFoldSize(200).setInputCol("uniqueFeatures")
    df = filter.fit(df).transform(df)
    df = folder.transform(df)


    import org.apache.spark.ml.regression.RandomForestRegressor
    val rf = new RandomForestRegressor().setMaxDepth(10).setNumTrees(50).setFeaturesCol(folder.getOrDefault(folder.outputCol))

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2))

    val rfModel = rf.fit(training)
    info(s"Model summary ${rfModel.explainParams()}")

    var predictDf = rfModel.transform(test)
    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDf)

    info(s"rmse is $rmse")

    val c = predictDf.stat.corr("label", "prediction")
    info(s"correlation coefficient $c")


    it("should have a model rmse of about 0.51") {
      rmse should be(0.51 +- 0.1)
    }

    it("should have a model correlation of about 0.84") {
      c should be(0.84 +- 0.1)
    }
  }
}
