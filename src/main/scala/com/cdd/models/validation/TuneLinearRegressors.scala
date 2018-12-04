package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.estimator.LinearRegressor
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.TestSystems.TestSystemDir
import Tuner._


object TuneLinearRegressorsDescriptors extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val lr = new LinearRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, lr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(lr.elasticNetParam, Vector(0.0, 0.25, 0.5, 0.75, 1.0))
    .addGrid(lr.maxIter, Vector(10, 100, 500))
    .addGrid(lr.lambda, Vector(1.0, 0.1, 0.05, 0.01, 0.005))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "rdkit_descriptors" -> "rdkit_desc", "cdk_descriptors" -> "cdk_desc")
  runTuning("tune_lr_el_desc.csv", "LR_EL_DESC", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneLinearRegressorsSparseFp extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val lr = new LinearRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, lr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(lr.maxIter, Vector(10, 100, 500))
    .addGrid(lr.elasticNetParam, Vector(0.0, 0.25, 0.5, 0.75, 1.0))
    .addGrid(lr.lambda, Vector(1.0, 0.1, 0.05, 0.01, 0.005))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_lr_el_fp.csv", "LR_EL_FP", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}



object TuneLinearRegressorsFoldedFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures").setFoldSize(1000)
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  val lr = new LinearRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, lr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(lr.maxIter, Vector(10, 100, 500))
    .addGrid(lr.elasticNetParam, Vector(0.0, 0.25, 0.5, 0.75, 1.0))
    .addGrid(lr.lambda, Vector(1.0, 0.1, 0.05, 0.01, 0.005))
    .addGrid(folder.foldSize, Vector(500, 1000, 1500, 2000))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_lr_el_fold_fp.csv", "LR_EL_FOLD_FP", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

