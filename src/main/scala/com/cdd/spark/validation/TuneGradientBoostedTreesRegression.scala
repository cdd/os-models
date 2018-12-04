package com.cdd.spark.validation

import com.cdd.models.utils._
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, PositiveLabelTransformer, VectorFolder}
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}


object TuneGradientBoostedTreesRegression extends App {


  /*
  val additionalSparkSettings = HashMap[String, String](
    "spark.yarn.am.memory" -> "20G",
    "spark.driver.memory" -> "20g",
    "spark.executor.memory" -> "20G",
    "spark.executors.cores" -> "2",
    "spark.executors.instance" -> "1",
    "spark.driver.cores" -> "1",
    "spark.yarn.maxAppAttempts" -> "1",
    "spark.yarn.driver.memoryOverhead" -> "2G",
    "spark.yarn.executor.memoryOverhead" -> "2G",
    "spark.driver.memoryOverhead" -> "2G",
    "spark.executor.memoryOverhead" -> "2G",
    "spark.memory.storageFraction" -> "0.15",
    "spark.memory.fraction" -> "0.5"
  )
  Configuration.additionalSparkSettings = Some(additionalSparkSettings)
*/
  val trf = new TuneGradientBoostedTreesRegression(false, args.toList)
  trf.tune

}

/**
  * Created by gjones on 7/12/17.
  */
class TuneGradientBoostedTreesRegression(removeExistingResults: Boolean = true, names: List[String] = List()) extends  Tuner with HasLogging {

  override def tune: Unit = {

    val gbt = new GBTRegressor()
      .setLabelCol("label").setFeaturesCol("uniqueFeatures")setSeed(1234L)

    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline().setStages(Array(filter, gbt))
    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_fp", "rdkit_fp", "cdk_desc", "rdkit_desc"))
      .addGrid(gbt.maxIter, Array(5, 10, 50))
      .addGrid(gbt.maxDepth, Array(5, 10, 20))
      .addGrid(gbt.lossType, Array("squared", "absolute"))
      .build()

    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)

    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile("tune_gbt.parquet")

    val func = (name: String, ds: DataFrame) => {

      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "fingerprints_CDK_FCFP6", "fingerprints_RDKit_FCFP6", "cdk_descriptors", "rdkit_descriptors")
        .toDF("no", "label", "cdk_fp", "rdkit_fp", "cdk_desc", "rdkit_desc")
        .filter("label is not null and cdk_fp is not null and rdkit_fp is not null and cdk_desc is not null and rdkit_desc is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}");

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Int, Int, String, Double, Double, Double, Double)] = combined
        .map { case ((p, rmse), m) =>
          (name, "GBT", p(filter.inputCol), p(gbt.maxIter), p(gbt.maxDepth), p(gbt.lossType), rmse, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "maxIter", "maxDepth", "lossType", "rmse", "f1", "f2", "f3")
      this.synchronized {
        resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
      }
    }: Unit

    val excludeNames = currentResults match { case Some(r) => r.toList case _ => List()}
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.CONTINUOUS, true, func)
    // testSystems.applyFunctionToTestSystemsConcurrently(includeNames = names, excludeNames=excludeNames)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = names, excludeNames=excludeNames)

    logger.info("Done!")
  }
}
