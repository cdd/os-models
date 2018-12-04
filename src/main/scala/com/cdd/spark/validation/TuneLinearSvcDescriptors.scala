
package com.cdd.spark.validation

import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame


object TuneLinearSvcDescriptors extends App {

  val tune = new TuneLinearSvcDescriptors(false)
  tune.tune()
}

class TuneLinearSvcDescriptors(removeExistingResults: Boolean = false, names: List[String] = List(), scale:Boolean=true) extends Tuner with HasLogging{

  override def tune(): Unit = {

    val svc = new LinearSVC()
    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline()
    var methodName: String = null
    var fileName: String = null
    if (scale) {
      val scaler = new StandardScaler().setInputCol("uniqueFeatures").setOutputCol("scaledFeatures").setWithMean(true).setWithStd(true)
      pipeline.setStages(Array(filter, scaler, svc))
      svc.setFeaturesCol("scaledFeatures")
      methodName = "SVC_DESCS"
      fileName = "tune_lsvc_descs.parquet"
    } else {
      pipeline.setStages(Array(filter, svc))
      svc.setFeaturesCol("uniqueFeatures")
      methodName = "SVC_DESC"
      fileName = "tune_lsvc_desc.parquet"
    }

    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_desc", "rdkit_desc"))
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
      var df = ds.select("no", "activity_value", "cdk_descriptors", "rdkit_descriptors")
        .toDF("no", "label", "cdk_desc", "rdkit_desc")
        .filter("label is not null and cdk_desc is not null and rdkit_desc is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Int, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, auc), m) =>
          (name, methodName, p(filter.inputCol), p(svc.maxIter), p(svc.regParam), auc, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "maxIter", "reg", "auc", "f1", "f2", "f3")
      resultsDf.write.mode("append").option("header", "true").parquet(resultsFile)
    }: Unit

    val excludeNames = currentResults match {
      case Some(r) => r.toList
      case _ => List()
    }
    val testSystems = SparkTestSystems.testSystemsFactory(TestSystems.TestSystemDir.DISCRETE, true, func)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = names, excludeNames = excludeNames)

    logger.info("Done!")
  }

}
