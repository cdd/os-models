package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.scalatest.{FunSpec, Matchers}

class RandomForestRegressionOnRdkitDescriptorsSpec extends FunSpec with Matchers{

 describe("Running a random forest regression model on RDKIT properties") {

   val sparkSession = Configuration.sparkSession
   var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/MalariaGSK.parquet").getAbsolutePath)
   df = DataFrameOps.filterForEstimator(df, "rdkit_descriptors")


   it("should have loaded 13403 molecules") {
     df.count() should be(13403)
   }


   var filter = new FilterSameFeatureValues().setInputCol("features").setOutputCol("uniqueFeatures")
   var rf = new RandomForestRegressor().setFeaturesCol("uniqueFeatures").setLabelCol("label").setSeed(1234L).setNumTrees(10).setMaxDepth(10)
   val pipeline = new Pipeline().setStages(Array(filter, rf))

   val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed = 1234L)
   val evaluator = new RegressionEvaluator()

   val pipelineModel = pipeline.fit(training)
   var predictDf = pipelineModel.transform(test)
   val rmse = evaluator.evaluate(predictDf)

   info(s"rmse is $rmse")

    it("should have a test model rmse of about 0.31") {
      rmse should be(0.31 +- 0.1)
    }

   val c = predictDf.stat.corr("label", "prediction")
   info(s"correlation coefficient $c")
 }
}
