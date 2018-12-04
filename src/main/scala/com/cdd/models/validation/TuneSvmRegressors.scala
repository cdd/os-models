package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.SvmRegressor
import com.cdd.models.pipeline.transformer._
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.validation.Tuner._

object TuneSvmRegressorTanimoto extends App {
  val filter = new FilterSameFeatureValues()
  val svm = new SvmRegressor().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures")
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

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svr_tanimoto_fp.csv", "SVR_TAN", optimizer, mapper, args.toList)
}

object TuneSvmRegressorRbfFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmRegressor().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(folder.foldSize, Vector(1000, 1500, 2000))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svr_rbf_fp.csv", "SVR_RBF_FP", optimizer, mapper, args.toList)
}

object TuneSvmRegressorRbfDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmRegressor().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_svr_rbf_desc.csv", "SVR_RBF_DESC", optimizer, mapper, args.toList)
}

object TuneSvmRegressorPolynomialFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  // setting degree higher than 2 results in failure to converge for many systems
  val svr = new SvmRegressor().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(folder.foldSize, Vector(1000, 1500, 2000))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1))
    .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
    .addGrid(svr.degree, Vector(2, 3))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svr_poly_fp.csv", "SVR_POLY_FP", optimizer, mapper, args.toList)
}


object TuneSvmRegressorPolynomialDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmRegressor().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(1.0e-5, 1.0e-4, 10e-3, 1.0e-2, 0.1, 1))
    .addGrid(svr.coef0, Vector(2.0, 4.0, 6.0, 8.0))
    .addGrid(svr.degree, Vector(2, 3, 4))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_svr_poly_desc.csv", "SVR_POLY_DESC", optimizer, mapper, args.toList)
}