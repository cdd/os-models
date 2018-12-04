package com.cdd.spark.validation


import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, PositiveLabelTransformer}
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame

object TuneGeneralizedLinearRegressionDescriptors extends App {

  val tuner = new TuneGeneralizedLinearRegressionDescriptors(false, args.toList)
  tuner.tune
}

/**
  * Created by gjones on 7/12/17.
  */
class TuneGeneralizedLinearRegressionDescriptors(removeExistingResults: Boolean = true, names: List[String] = List()) extends Tuner with HasLogging {

  override def tune(): Unit = {

    val glr = new GeneralizedLinearRegression()
      .setLabelCol("positiveLabel")
      .setFeaturesCol("normFeatures")
    val filter = new FilterSameFeatureValues().setInputCol("features").setOutputCol("filteredFeatures")
    val scaler = new StandardScaler()
      .setInputCol("filteredFeatures")
      .setOutputCol("normFeatures")
      .setWithMean(true).setWithStd(true)
    val pipeline = new Pipeline().setStages(Array(filter, scaler, glr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_desc", "rdkit_desc"))
      .addGrid(glr.family, Array("gaussian", "gamma"))
      .addGrid(glr.maxIter, Array(10, 50, 100))
      .addGrid(glr.regParam, Array(0.1, 0.3, 0.5, 0.7))
      .build()

    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol("positiveLabel"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)

    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile("tune_glr_desc.parquet")

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "cdk_descriptors", "rdkit_descriptors")
        .toDF("no", "label", "cdk_desc", "rdkit_desc")
        .filter("label is not null and cdk_desc is not null and rdkit_desc is not null")
      df = (new PositiveLabelTransformer()).transform(df)
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, String, Int, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, rmse), m) =>
          (name, "GLR_DESC", p(filter.inputCol), p(glr.family), p(glr.maxIter), p(glr.regParam), rmse, m(0), m(1), m(2))}
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "family", "maxIter", "reg", "rmse", "f1", "f2", "f3")
      this.synchronized {
        resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
      }
    }: Unit

    val excludeNames = currentResults match { case Some(r) => r.toList case _ => List()}
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.CONTINUOUS, true, func)
    // testSystems.applyFunctionToTestSystemsConcurrently(includeNames = names, excludeNames=excludeNames)
    testSystems.applyFunctionToTestSystemsConcurrently(includeNames = names, excludeNames=excludeNames)

    logger.info("Done!")
  }
}

