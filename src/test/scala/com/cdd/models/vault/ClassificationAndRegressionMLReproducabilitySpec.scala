package com.cdd.models.vault

import com.cdd.models.pipeline.estimator.SmileUtil
import com.cdd.models.pipeline.{ClassificationEvaluator, ParameterMap}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class ClassificationAndRegressionMLReproducabilitySpec extends FunSpec with Matchers {
 describe("Validating that ML estimators are reproducible") {
   val dataTable = ClassificationAndRegressionML.buildDataTable()
   val estimator = ClassificationAndRegressionML.createOptimizedEstimators()(3)

   it ("should be testing the Smile SVC linear classifier") {
     estimator.getEstimatorName() should be("Smile SVC Linear classification")
   }

   for (no <- 1 to 10) {
     val evaluator = new ClassificationEvaluator().setMetric("f1")

     val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(estimator).setNumFolds(3)
     dataSplit.foldAndFit(dataTable)
     val f1 = evaluator.evaluate(dataSplit.getPredictDt())

     it(s"should have an f1 value of 0.857 for replicate test $no") {
       f1 should be(0.857 +- 0.001)
     }

     SmileUtil.resetSmileMathRandom()
   }
 }
}
