package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class LibLinearClassifierSpec extends FunSpec with Matchers{

   describe("Running a Linear SVM classification model on RDKIT fingerprints") {

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

      val svm = new LibLinearClassifier()
        .setFeaturesColumn("foldedFeatures")
        .setEps(0.001)
        .setRegularizationType("L2")
        .setMaxIterations(100)
        .setC(0.1)
      val svmModel = svm.fit(trainDt)

      val predictDt = svmModel.transform(testDt)

      val evaluator = new ClassificationEvaluator().setMetric("accuracy")
      val accuracy = evaluator.evaluate(predictDt)

      val auc = evaluator.rocAuc(predictDt)
      it("should have a test model accuracy of about 0.70") {
        accuracy should be(0.70 +- 0.1)
      }

      it("should have a test model rocAuc of about 0.74") {
        auc should be(0.74 +- 0.1)
      }

      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svm)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
      info(s"AUC is ${auc2}")

      it("should have a datasplit model accuracy of about 0.71") {
        dataSplit.getMetric() should be(0.71 +- 0.1)
      }

      it("should have a datasplit model AUC of about 0.74") {
        auc2 should be(0.74 +- 0.1)
      }

      info("Done")
    }
}
