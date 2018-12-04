package com.cdd.models.pipeline.tuning

import java.io.File

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ParameterMap, Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.estimator.SvmRegressor
import com.cdd.models.pipeline.transformer.VectorFolder
import org.scalatest.{FunSpec, Matchers}

class HyperparameterOptimizerSpec extends FunSpec with Matchers {
  describe("Tuning an SVR classification model") {

    var dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz")

    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp")).filterNull().shuffle()

    it("should have loaded 741 molecules") {
      dt.length should be(741)
    }

    val folder = new VectorFolder()
    val svm = new SvmRegressor().setKernelType("RBF").setGamma(0.01).setFeaturesColumn("foldedFeatures")
    val grid = new ParameterGridBuilder()
      .addGrid(folder.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
      .addGrid(folder.foldSize, Vector(100, 200))
      .addGrid(svm.gamma, Vector(0.01, 0.1))
      .build()
    val pipeline = new Pipeline().setStages(Vector(folder, svm))
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setNumFolds(2).setParameterGrid(grid)

    val results = optimizer.optimizeParameters(dt)

    it("should have the right parameter values") {
      var pm = new ParameterMap().put(folder.featuresColumn, "rdkit_fp").put(folder.foldSize, 100).put(svm.gamma, 0.1)
      var metric = results.getMetric(pm)
      metric should not be (None)
      metric.get should be(0.788 +- 0.01)

      pm = new ParameterMap().put(folder.featuresColumn, "cdk_fp").put(folder.foldSize, 200).put(svm.gamma, 0.01)
      metric = results.getMetric(pm)
      metric should not be (None)
      metric.get should be(0.498 +- 0.01)
    }

    val file = new File("/tmp/chagas_svr.csv")
    if (file.exists)
      file.delete()
    results.logToFile(file.getAbsolutePath, "Chagas", "SVR")

    it ("should have written a log file") {
      file.exists() should be(true)
    }

    info("Done")
  }
}
