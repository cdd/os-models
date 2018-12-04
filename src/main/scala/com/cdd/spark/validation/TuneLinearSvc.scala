package com.cdd.spark.validation

import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification
import org.apache.spark.ml.feature.StandardScaler


object TuneLinearSvc extends App {

  val tune = new TuneLinearSvc(false)
  tune.tune()
}

class TuneLinearSvc(removeExistingResults: Boolean = false, names: List[String] = List(), scale:Boolean = true) extends Tuner with HasLogging {


  override def tune(): Unit = {

    val svc = new LinearSVC()
    val folder = new VectorFolder().setInputCol("uniqueFeatures")
    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline()
    var methodName: String = null
    var fileName: String = null
    if (scale) {
      val scaler = new StandardScaler().setInputCol("foldedFeatures").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true)
      pipeline.setStages(Array(filter, folder, scaler, svc))
      svc.setFeaturesCol("scaledFeatures")
      methodName = "SVC_FPS"
      fileName = "tune_lsvc_fps.parquet"
    } else {
      pipeline.setStages(Array(filter, folder, svc))
      svc.setFeaturesCol("foldedFeatures")
      methodName = "SVC_FP"
      fileName = "tune_lsvc_fp.parquet"
    }

    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_fp", "rdkit_fp"))
      .addGrid(folder.foldSize, Array(1000, 1500, 2000))
      .addGrid(svc.maxIter, Array(10, 50, 100))
      .addGrid(svc.regParam, Array(1, 0.1, 0.03, 0.01))
      .build()

    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC").setRawPredictionCol("rawPrediction").setLabelCol("label")
    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)


    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile(fileName)

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "fingerprints_CDK_FCFP6", "fingerprints_RDKit_FCFP6")
        .toDF("no", "label", "cdk_fp", "rdkit_fp")
        .filter("label is not null and cdk_fp is not null and rdkit_fp is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Int, Int, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, auc), m) =>
          (name, methodName, p(filter.inputCol), p(folder.foldSize), p(svc.maxIter), p(svc.regParam), auc, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "nfolds", "maxIter", "reg", "auc", "f1", "f2", "f3")
      resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
    }: Unit

    val excludeNames = currentResults match {
      case Some(r) => r.toList
      case _ => List()
    }
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.DISCRETE, true, func)
    // testSystems.applyFunctionToTestSystemsConcurrently(names = names, excludeNames=excludeNames)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = names, excludeNames = excludeNames)

    logger.info("Done!")
  }

}
