package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.RegressionEvaluator
import com.cdd.models.pipeline.transformer.{MaxMinScaler, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import org.scalatest.{FunSpec, Matchers}

class EpochNeuralNetworkRegressorSpec extends FunSpec with Matchers {
   describe("Running an Epoch neural net regressor on SVM on RDKIT fingerprints") {
     var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

     dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "features")).filterNull().shuffle()

     it("should have loaded 741 molecules") {
       dt.length should be(741)
     }

     val folder = new VectorFolder().setFoldSize(500)
     dt = folder.transform(dt)
     val normalizer = new MaxMinScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("scaledFeatures")
     dt = normalizer.fit(dt).transform(dt)
     val (testDt, trainDt) = dt.testTrainSplit()

     val nnr = new EpochNeuralNetworkRegressor().setFeaturesColumn("scaledFeatures").setLamdba(1.0).setNoEpochs(200).setLayers(Vector(75))
     val nnrModel = nnr.fit(trainDt)

     val predictDt = nnrModel.transform(testDt)

     val evaluator = new RegressionEvaluator()
     val rmse = evaluator.evaluate(predictDt)

     val c = evaluator.correlation(predictDt)

     it("should have a test model rmse of about 0.78") {
       rmse should be(0.78 +- 0.1)
     }

     it("should have a test model correlation of about 0.62") {
       c should be(0.62 +- 0.1)
     }

     val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(nnr)
     dataSplit.foldAndFit(dt)

     info(s"Metric is ${dataSplit.getMetric()}")
     val c2 = evaluator.correlation(dataSplit.getPredictDt())
     info(s"Correlation is ${c2}")

     it("should have a datasplit model rmse of about 0.75") {
       dataSplit.getMetric() should be(0.75 +- 0.1)
     }

     it("should have a datasplit model correlation of about 0.66") {
       c2 should be(0.66 +- 0.1)
     }

     info("Done")
   }
}
