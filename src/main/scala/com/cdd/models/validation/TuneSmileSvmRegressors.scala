package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.SmileSvmRegressor
import com.cdd.models.pipeline.transformer._
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.validation.Tuner._

object TuneSmileSvmRegressorTanimoto extends App {
  val filter = new FilterSameFeatureValues()
  val svm = new SmileSvmRegressor().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, svm))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(svm.coef0, Vector(0.0, 1.0, 2.0, 3.0))
    .addGrid(svm.degree, Vector(2, 3, 4, 5))
    .addGrid(svm.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_smile_svr_tanimoto_fp.csv", "SMILE_SVR_TAN", optimizer, mapper, args.toList)
}

object TuneSmileSvmRegressorRbf extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmRegressor().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp", "cdk_descriptors" -> "cdk_desc",
    "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_svr_rbf.csv", "SMILE_SVR_RBF", optimizer, mapper, args.toList)
}


object TuneSmileSvmRegressorPolynomial extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmRegressor().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures").setMaxIter(2000)
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1))
    .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
    .addGrid(svr.degree, Vector(2, 3))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp", "cdk_descriptors" -> "cdk_desc",
    "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_svr_poly.csv", "SMILE_SVR_POLY", optimizer, mapper, args.toList)
}

