package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.spark.pipeline.transformer.FilterSameFeatureValuesSpec
import org.scalatest.{FunSpec, Matchers}

class SvmClassifierSpec extends FunSpec with Matchers {

    describe("Running a SVM classification model on RDKIT fingerprints") {

      var dt = DataTable.loadProjectFile("data/discrete/data_chagas.csv.gz")

      dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)

      it("should have loaded 4055 molecules") {
        dt.length should be(4055)
      }

      val filter = new FilterSameFeatureValues()
      val folder = new VectorFolder().setFoldSize(200).setFeaturesColumn("uniqueFeatures")
      dt = filter.fit(dt).transform(dt)
      dt = folder.transform(dt)
      val (testDt, trainDt) = dt.testTrainSplit()

      val svm = new SvmClassifier().setKernelType("RBF").setGamma(0.01).setFeaturesColumn("foldedFeatures")
      val svmModel = svm.fit(trainDt)

      val predictDt = svmModel.transform(testDt)

      val evaluator = new ClassificationEvaluator().setMetric("accuracy")
      val accuracy = evaluator.evaluate(predictDt)

      val auc = evaluator.rocAuc(predictDt)
      it("should have a test model accuracy of about 0.77") {
        accuracy should be(0.77 +- 0.1)
      }

      it("should have a test model rocAuc of about 0.79") {
        auc should be(0.79 +- 0.1)
      }

      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svm)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
      info(s"AUC is ${auc2}")

      it("should have a datasplit model accuracy of about 0.77") {
        dataSplit.getMetric() should be(0.77 +- 0.1)
      }

      it("should have a datasplit model AUC of about 0.80") {
        auc2 should be(0.80 +- 0.1)
      }

      info("Done")
    }

}
