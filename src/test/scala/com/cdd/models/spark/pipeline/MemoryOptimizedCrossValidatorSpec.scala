package com.cdd.models.spark.pipeline

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.estimator.LibSvmRegressionEstimator
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.scalatest.{FunSpec, Matchers}

/**
  * A spec that tests that the memory optimizer cross validator get the same results as the regular cross validator
  *
  * Created by gjones on 7/12/17.
  */
class MemoryOptimizedCrossValidatorSpec extends FunSpec with Matchers {

  describe("Cross validation using memory optimized methods") {
    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    df = DataFrameOps.filterForEstimator(df, "fingerprints_RDKit_FCFP6").cache()
    val svr = new LibSvmRegressionEstimator()
      .setLabelCol("label")
      .setFeaturesCol("foldedFeatures")
      .setForceCoalesce(true)
    val folder = new VectorFolder()
    folder.setFoldSize(200)
    val pipeline = new Pipeline().setStages(Array(folder, svr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(svr.C, Array(0.01, 0.1))
      .addGrid(svr.gamma, Array(0.01, 0.1))
      .build()
    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)

    val summary = cv.optimizeParameters(df)
    val summary2 = cv.fit(df)

    it ("should have the same metrics for each parameter optimization method") {
      summary.avgMetrics.zip(summary2.avgMetrics).foreach { case(m1, m2) =>
          m1 should be(m2 +- 1e-8)
      }
    }

    df.unpersist()
  }
}
