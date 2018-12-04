package com.cdd.spark.validation


import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame


object TuneLinearRegressionDescriptors extends App {


  val tuner = new TuneLinearRegressionDescriptors
  tuner.tune()

}

class TuneLinearRegressionDescriptors(removeExistingResults: Boolean = false, names: List[String] = List(), scale: Boolean = true)
  extends Tuner with HasLogging{

  override def tune(): Unit = {

    val lr = new LinearRegression()
    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline().setStages(Array(filter, lr))
    var methodName: String = null
    var fileName: String = null
    if (scale) {
      val scaler = new StandardScaler().setInputCol("uniqueFeatures").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true)
      pipeline.setStages(Array(filter, scaler, lr))
      lr.setFeaturesCol("scaledFeatures")
      methodName = "LR_DESCS"
      fileName = "tune_lr_descs.parquet"
    } else {
      pipeline.setStages(Array(filter, lr))
      lr.setFeaturesCol("uniqueFeatures")
      methodName = "LR_DESC"
      fileName = "tune_lr_desc.parquet"
    }

    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_desc", "rdkit_desc"))
      .addGrid(lr.elasticNetParam, Array(0.7, 0.8, 0.9))
      .addGrid(lr.maxIter, Array(10, 50, 100))
      .addGrid(lr.regParam, Array(3, 1, 0.1, 0.03, 0.01))
      .build()

    val evaluator = new RegressionEvaluator()
    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)

    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile(fileName)

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "cdk_descriptors", "rdkit_descriptors")
        .toDF("no", "label", "cdk_desc", "rdkit_desc")
        .filter("label is not null and cdk_desc is not null and rdkit_desc is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Double, Int, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, rmse), m) =>
          (name, methodName, p(filter.inputCol), p(lr.elasticNetParam), p(lr.maxIter), p(lr.regParam), rmse, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "elasticNetParam", "maxIter", "reg", "rmse", "f1", "f2", "f3")
      resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
    }: Unit

    val excludeNames = currentResults match {
      case Some(r) => r.toList
      case _ => List()
    }
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.CONTINUOUS, true, func)
    // testSystems.applyFunctionToTestSystemsConcurrently(names = names, excludeNames=excludeNames)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = names, excludeNames = excludeNames)

    logger.info("Done!")
  }
}
