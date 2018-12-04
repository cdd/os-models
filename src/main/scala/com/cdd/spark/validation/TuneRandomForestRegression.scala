package com.cdd.spark.validation

import com.cdd.models.utils._
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame


object TuneRandomForestRegression extends App {

  val trf = new TuneRandomForestRegression(false, args.toList)
  trf.tuneRandomForestRegressionOverAllContinuous()
}


/**
  * Created by gjones on 6/14/17.
  */
class TuneRandomForestRegression(removeExistingResults: Boolean = false, names: List[String] = List()) extends HasLogging {

  def tuneRandomForestRegressionOverAllContinuous(): Unit = {

    val rf = new RandomForestRegressor().setFeaturesCol("uniqueFeatures").setLabelCol("label").setSeed(1234L)
    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline().setStages(Array(filter, rf))
    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_fp", "rdkit_fp", "cdk_desc", "rdkit_desc"))
      .addGrid(rf.maxDepth, Array(2, 5, 10, 15, 20, 30))
      .addGrid(rf.numTrees, Array(5, 10, 20, 30, 40, 50, 60, 70))
      .build()

    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile("tune_random_forest.parquet")

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "fingerprints_CDK_FCFP6", "fingerprints_RDKit_FCFP6", "cdk_descriptors", "rdkit_descriptors")
        .toDF("no", "label", "cdk_fp", "rdkit_fp", "cdk_desc", "rdkit_desc")
        .filter("label is not null and cdk_fp is not null and rdkit_fp is not null and cdk_desc is not null and rdkit_desc is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Int, Int, Double, Double, Double, Double)] = combined
        .map { case ((p, rmse), m) =>
          (name, "RandomForest", p(filter.inputCol), p(rf.maxDepth), p(rf.numTrees), rmse, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "depth", "trees", "rmse", "f1", "f2", "f3")
      resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
    }: Unit

    val excludeNames = currentResults match {
      case Some(r) => r.toList
      case _ => List()
    }
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.CONTINUOUS, true, func)
    testSystems.applyFunctionToTestSystemsConcurrently(includeNames = names, excludeNames = excludeNames)

    logger.info("Done!")
  }

}
