package com.cdd.models.spark.pipeline.estimator

import com.cdd.models.molecule.FingerprintTransform._
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.utils.Configuration
import com.cdd.spark.pipeline.{ClassifierDataSplit, RdkitInputBuilder}
import com.cdd.spark.pipeline.estimator.LibSvmClassificationEstimator
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/12/17.
  */
class LibSvmClassificationEstimatorSpec extends FunSpec with Matchers {

  describe("Running a SVM classification model on RDKIT fingerprints") {
    var builder = new RdkitInputBuilder("data/discrete/data_chagas.sdf.gz")
      .setActivityField("Activity")
      .build(Configuration.sparkSession)
      .addFingerprintColumn(RdkitFingerprintClass.ECFP6, Fold, 200)
    var df = builder.getDf()
    df = DataFrameOps.filterForEstimator(df, builder.firstFingerprintColumn()).coalesce(1)

    it("should have loaded 4061 molecules") {
      df.count() should be(4061)
    }

    val Array(training, test) = df.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val svmClassifier = new LibSvmClassificationEstimator().setFeaturesCol("features").setKernelType("RBF").setGamma(0.01)

    val model = svmClassifier.fit(training)
    val predictions = model.transform(test)

    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
    var accuracy = evaluator.evaluate(predictions)
    info(s"accuracy is ${accuracy}")

    it ("should have an accuracy value of about 0.76") {
      accuracy should be(0.76 +- 0.1)
    }

    val dataSplit = new ClassifierDataSplit().setEstimator(svmClassifier).setEvaluator(evaluator)
    dataSplit.probabilityColumn = "rawPrediction"
    dataSplit.foldAndFit(df)
    val predictDf = dataSplit.getPredictDf()

    val rocDf = dataSplit.rocDf()
    val rocAuc = dataSplit.getBinaryClassificationMetrics().areaUnderROC()
    accuracy = dataSplit.getMetric()

    it ("should have a ROCAuc value of about 0.78 ") {
      rocAuc should be(0.78 +- 0.1)
    }

    it ("should have an accuracy value of about 0.74 ") {
      accuracy should be(0.74 +- 0.1)
    }

    info(s"Metric is ${dataSplit.getMetric()}")

  }
}
