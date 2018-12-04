package com.cdd.spark.validation

import com.cdd.models.utils.{Configuration, HasLogging, SparkTestSystems, TestSystems}
import com.cdd.spark.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.spark.pipeline.tuning.MemoryOptimizedCrossValidator
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame

object TuneNaiveBayes extends App {
  val tuner = new TuneNaiveBayes(false)
  tuner.tune()
}

class TuneNaiveBayes(removeExistingResults: Boolean = false, names: List[String] = List()) extends Tuner with HasLogging{

  override def tune(): Unit = {
    tuneNaiveBayes(false)
    tuneNaiveBayes(true)
  }

  def tuneNaiveBayes(multinomial: Boolean): Unit = {

    val nb = new NaiveBayes()
    val filter = new FilterSameFeatureValues()
    val pipeline = new Pipeline()

    if (multinomial) {
      nb.setModelType("multinomial")
      pipeline.setStages(Array(filter, nb))
      nb.setFeaturesCol("uniqueFeatures")
    } else {
      nb.setModelType("bernoulli")
      val binarizer: Binarizer = new Binarizer()
        .setInputCol("uniqueFeatures")
        .setOutputCol("binarizedFeatures")
      nb.setFeaturesCol("binarizedFeatures")
      pipeline.setStages(Array(filter, binarizer, nb))

    }

    val paramGrid = new ParamGridBuilder()
      .addGrid(filter.inputCol, Array("cdk_ecfp", "rdkit_ecfp", "cdk_fcfp", "rdkit_fcfp"))
      .addGrid(nb.smoothing, Array(0.2, 1.0, 5, 10, 25))
      .build()

    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC").setRawPredictionCol("probability")

    val cv = new MemoryOptimizedCrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
      .setSeed(1234L)

    val modelName = if (multinomial) "mn" else "bn"
    val (resultsFile, currentResults) = SparkTestSystems.prepareResultFile(s"tune_nb_${modelName}.parquet")

    val func = (name: String, ds: DataFrame) => {
      logger.warn(s"Running CV on ${name}")
      var df = ds.select("no", "activity_value", "fingerprints_CDK_ECFP6", "fingerprints_RDKit_ECFP6", "fingerprints_CDK_FCFP6", "fingerprints_RDKit_FCFP6")
        .toDF("no", "label", "cdk_ecfp", "rdkit_ecfp", "cdk_fcfp", "rdkit_fcfp")
        .filter("label is not null and cdk_ecfp is not null and rdkit_ecfp is not null and cdk_fcfp is not null and rdkit_fcfp is not null")
      df = df.cache()
      logger.warn(s"Loaded dataframe for ${name}")
      val summary = cv.optimizeParameters(df)
      df.unpersist()
      logger.warn(s"Finished hyperparameter CV ${name}")

      val combined = paramGrid.zip(summary.avgMetrics).zip(summary.splitMetrics)

      val results: Seq[(String, String, String, Double, Double, Double, Double, Double)] = combined
        .map { case ((p, auc), m) =>
          (name, s"NB_${modelName.toUpperCase}", p(filter.inputCol), p(nb.smoothing), auc, m(0), m(1), m(2))
        }
      val resultsDf = ds.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(results, 1))
        .toDF("name", "method", "feature", "smoothing", "auc", "f1", "f2", "f3")
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
