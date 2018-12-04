package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SmileSvmClassifierSpec  extends FunSpec with Matchers{

  describe("Running Smile SVC classifiers on dense and sparse RDKit fingerprints") {

    var loadTable = DataTable.loadProjectFile("data/discrete/data_chagas.csv.gz")
    loadTable = loadTable.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 4321L)

    it("should have loaded 4055 molecules") {
      loadTable.length should be(4055)
    }

    val filter = new FilterSameFeatureValues()
    val dataTable = filter.fit(loadTable).transform(loadTable)

    describe("Running a Smile SVC on sparse fingerprints with a Tanimoto kernel") {

      val (testDt, trainDt) = dataTable.testTrainSplit()

      val svc = new SmileSvmClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setC(1.0)
        .setKernelType("Tanimoto")
        .setGamma(1.0)
        .setDegree(3)
        .setCoef0(1.0)
      val svcModel = svc.fit(trainDt)

      val predictDt = svcModel.transform(testDt)

      val evaluator = new ClassificationEvaluator().setMetric("accuracy")
      val accuracy = evaluator.evaluate(predictDt)
      val auc = evaluator.rocAuc(predictDt)
      it("should have a test model accuracy of about 0.78") {
        accuracy should be(0.78 +- 0.1)
      }

      it("should have a test model rocAuc of about 0.82") {
        auc should be(0.80 +- 0.1)
      }

      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svc)
      dataSplit.foldAndFit(dataTable)

      info(s"Metric is ${dataSplit.getMetric()}")
      val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
      info(s"AUC is ${auc2}")

      it("should have a datasplit model accuracy of about 0.77") {
        dataSplit.getMetric() should be(0.77 +- 0.1)
      }

      it("should have a datasplit model AUC of about 0.82") {
        auc2 should be(0.82 +- 0.1)
      }

      info("Done")
    }



    describe("Running a Smile SVC on sparse fingerprints with a RBF kernel") {

      val (testDt, trainDt) = dataTable.testTrainSplit()

      val svc = new SmileSvmClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setC(1.0)
        .setKernelType("RBF")
        .setGamma(0.01)
      val svcModel = svc.fit(trainDt)

      val predictDt = svcModel.transform(testDt)

      val evaluator = new ClassificationEvaluator().setMetric("accuracy")
      val accuracy = evaluator.evaluate(predictDt)
      val auc = evaluator.rocAuc(predictDt)
      it("should have a test model accuracy of about 0.77") {
        accuracy should be(0.77 +- 0.1)
      }

      it("should have a test model rocAuc of about 0.80") {
        auc should be(0.80 +- 0.1)
      }

      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svc)
      dataSplit.foldAndFit(dataTable)

      info(s"Metric is ${dataSplit.getMetric()}")
      val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
      info(s"AUC is ${auc2}")

      it("should have a datasplit model accuracy of about 0.76") {
        dataSplit.getMetric() should be(0.76 +- 0.1)
      }

      it("should have a datasplit model AUC of about 0.81") {
        auc2 should be(0.81 +- 0.1)
      }

      info("Done")
    }


    describe("Running a Smile SVC classification model on dense folded fingerprints with a RBF kernel") {


      val folder = new VectorFolder().setFoldSize(1000).setFeaturesColumn("uniqueFeatures")
      var dt = folder.transform(dataTable)

      val (testDt, trainDt) = dt.testTrainSplit()

      val svc = new SmileSvmClassifier()
        .setC(1.0)
        .setKernelType("RBF")
        .setGamma(0.01)
        .setFeaturesColumn("foldedFeatures")
      val svcModel = svc.fit(trainDt)

      val predictDt = svcModel.transform(testDt)

      val evaluator = new ClassificationEvaluator().setMetric("accuracy")
      val accuracy = evaluator.evaluate(predictDt)

      val auc = evaluator.rocAuc(predictDt)
      it("should have a test model accuracy of about 0.76") {
        accuracy should be(0.76 +- 0.1)
      }

      it("should have a test model rocAuc of about 0.81") {
        auc should be(0.81 +- 0.1)
      }

      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svc)
      dataSplit.foldAndFit(dt)

      info(s"Metric is ${dataSplit.getMetric()}")
      val auc2 = evaluator.rocAuc(dataSplit.getPredictDt())
      info(s"AUC is ${auc2}")

      it("should have a datasplit model accuracy of about 0.76") {
        dataSplit.getMetric() should be(0.76 +- 0.1)
      }

      it("should have a datasplit model AUC of about 0.81") {
        auc2 should be(0.81 +- 0.1)
      }

      info("Done")
    }
  }
}
