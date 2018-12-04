package com.cdd.models.spark.pipeline

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.pipeline.{CdkInputBuilder, ClassifierDataSplit}
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 5/22/17.
  */
class SimpleClassificationModelSpec extends FunSpec with Matchers {

  val sparkSession = Configuration.sparkSession
  var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/discrete/data_chagas.parquet").getAbsolutePath)
  df = DataFrameOps.filterForEstimator(df, "fingerprints_CDK_ECFP6")
  val folder = new VectorFolder().setFoldSize(200)
  df = folder.transform(df)

  describe("Running a classification model on a dataframe") {

    df.cache()
    val Array(training, test) = df.randomSplit(Array(0.8, 0.2))
    val bayes = new NaiveBayes().setFeaturesCol(folder.getOrDefault(folder.outputCol))
    val model = bayes.fit(training)
    val predictions = model.transform(test)
    var evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC").setRawPredictionCol("probability")
    val roc = evaluator.evaluate(predictions)

    info(s"roc is ${roc}")

    val evaluator2 = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    val accuracy = evaluator2.evaluate(predictions)
    info(s"accuracy is ${accuracy}")

    val probDf = predictions.select("probability", "label")
    val probInfo = probDf.rdd.map { r => (r.getAs[Vector](0)(1), r.getDouble(1)) }
    val metrics = new BinaryClassificationMetrics(probInfo)
    val rocRdd = metrics.roc()
    val rocDf = Configuration.sparkSession.createDataFrame(rocRdd).toDF("FPR", "TPR")

    info("Created roc points")

  }

  describe("Running a model on a dataframe split into test and training for classifcation") {

    it("should have an input dataframe of 4055 rows") {
      df.count() should be(4055)
    }

    val bayes = new NaiveBayes().setFeaturesCol(folder.getOrDefault(folder.outputCol))
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    val dataSplit = new ClassifierDataSplit().setEstimator(bayes).setEvaluator(evaluator)
    dataSplit.foldAndFit(df)
    val predictDf = dataSplit.getPredictDf()


    it("should have an output dataframe of 4055 rows") {
      predictDf.count() should be(4055)
    }

    it("should have a prediction and probability column") {
      predictDf.columns.toSet should equal(Set("no", "label", "features", "prediction", "rawPrediction", "probability", "foldedFeatures"))
    }

    it("should have a model accuracy of about 0.65") {
      dataSplit.getMetric() should be(0.65 +- 0.1)
    }

    it("should have a model roc AUC for about 0.7") {
      dataSplit.getBinaryClassificationMetrics().areaUnderROC() should be(0.7 +- 0.1)
    }

    val rocDf = dataSplit.rocDf()
    val rocAuc = dataSplit.getBinaryClassificationMetrics().areaUnderROC()
    val accuracy = dataSplit.getMetric()

    info(s"Metric is ${dataSplit.getMetric()}")
    info(s"Correlation is ${dataSplit.getCorrelation()}")

  }
}


