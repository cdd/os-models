package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator
import com.cdd.models.pipeline.transformer.FilterSameFeatureValues
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class SvmClassifierTanimotoSpec extends FunSpec with Matchers {
  describe("Running a SVM classifier model on RDKIT fingerprints with Tanimoto Kernel") {

    var dt = DataTable.loadProjectFile("data/discrete/data_chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle(seed = 1234L)

    it("should have loaded 4055 molecules") {
      dt.length should be(4055)
    }

    val filter = new FilterSameFeatureValues()
    dt = filter.fit(dt).transform(dt)
    val (testDt, trainDt) = dt.testTrainSplit()

    val svm = new SvmClassifier().setKernelType("TANIMOTO").setGamma(1).setCoef0(1).setDegree(3).setFeaturesColumn("uniqueFeatures")
    val svmModel = svm.fit(trainDt)

    val predictDt = svmModel.transform(testDt)


    val evaluator = new ClassificationEvaluator().setMetric("accuracy")
    val accuracy = evaluator.evaluate(predictDt)

    val aucData = evaluator.rocData(predictDt)
    val auc = evaluator.rocAuc(aucData)
    evaluator.plot("/tmp/svm_tan_classification_tts.png", aucData)
    it("should have a test model accuracy of about 0.77") {
      accuracy should be(0.77 +- 0.1)
    }

    it("should have a test model rocAuc of about 0.82") {
      auc should be(0.82 +- 0.1)
    }

    val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(svm)
    dataSplit.foldAndFit(dt)

    info(s"Metric is ${dataSplit.getMetric()}")
    val aucData2 = evaluator.rocData(dataSplit.getPredictDt())
    val auc2 = evaluator.rocAuc(aucData2)
    info(s"AUC is ${auc2}")

    it("should have a datasplit model accuracy of about 0.77") {
      dataSplit.getMetric() should be(0.77 +- 0.1)
    }

    it("should have a datasplit model AUC of about 0.82") {
      auc2 should be(0.82 +- 0.1)
    }

    evaluator.plot("/tmp/svm_tan_classification_ds.png", aucData2, "Chagas SVC with Tanimoto kernel")
    info("Done")
  }
}
