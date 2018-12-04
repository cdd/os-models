package com.cdd.spark.validation


import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, PositiveLabelTransformer, VectorFolder}
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame

object TuneGeneralizedLinearRegression extends App {

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
  val tuner = new TuneGeneralizedLinearRegression(false, args.toList)
  tuner.tune()

}


/**
  * Created by gjones on 7/12/17.
  */
class TuneGeneralizedLinearRegression(removeExistingResults: Boolean = true, names: List[String] = List(), scale:Boolean = true) extends Tuner with HasLogging{

  override def tune(): Unit = {

    val glr = new GeneralizedLinearRegression()
      .setLabelCol("positiveLabel")
    val folder = new VectorFolder().setInputCol("uniqueFeatures")
    val filter = new FilterSameFeatureValues()

    val pipeline = new Pipeline()
    var methodName:String = null
    var fileName:String = null

    if (scale) {
      val scaler = new StandardScaler().setInputCol("foldedFeatures").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true)
      pipeline.setStages(Array(filter, folder, scaler, glr))
      glr.setFeaturesCol("scaledFeatures")
      methodName = "GLR_FPS"
      fileName = "tune_glr_fps.parquet"
    } else {
      pipeline.setStages(Array(filter, folder, glr))
      glr.setFeaturesCol("foldedFeatures")
      methodName = "GLR_FP"
      fileName = "tune_glr_fp.parquet"
    }
    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_fp", "rdkit_fp"))
      .addGrid(folder.foldSize, Array(1000, 1500, 2000))
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


    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile(fileName)

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "fingerprints_CDK_FCFP6", "fingerprints_RDKit_FCFP6")
        .toDF("no", "label", "cdk_fp", "rdkit_fp")
        .filter("label is not null and cdk_fp is not null and rdkit_fp is not null")
      df = (new PositiveLabelTransformer()).transform(df)
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Int, String, Int, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, rmse), m) =>
          (name, methodName, p(filter.inputCol), p(folder.foldSize), p(glr.family), p(glr.maxIter), p(glr.regParam), rmse, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "nfolds", "family", "maxIter", "reg", "rmse", "f1", "f2", "f3")
      resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
    }: Unit

    val excludeNames = currentResults match { case Some(r) => r.toList case _ => List()}
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.CONTINUOUS, true, func)
    // testSystems.applyFunctionToTestSystemsConcurrently(names = names, excludeNames=excludeNames)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = names, excludeNames=excludeNames)

    logger.info("Done!")
  }
}
