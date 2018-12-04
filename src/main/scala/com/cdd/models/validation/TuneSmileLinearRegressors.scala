package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.estimator.{SmileLassoRegressor, SmileRidgeRegressor}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import Tuner._
import com.cdd.models.utils.TestSystems.TestSystemDir

object TuneSmileRidgeRegressorFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures")
  val rr = new SmileRidgeRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, rr))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(scaler.scalerType, Vector("Raw", "MaxMin", "Standard"))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rr.lambda, Vector(3, 1, 0.1, 0.03, 0.01))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_smile_rr_fp.csv", "SMILE_RR", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneSmileRidgeRegressorDesc extends App {

}

object TuneSmileLassoRegressorFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures")
  val lr = new SmileLassoRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, lr))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(scaler.scalerType, Vector("Raw", "MaxMin", "Standard"))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(lr.lambda, Vector(3, 1, 0.1, 0.03, 0.01))
    .addGrid(lr.maxIter, Vector(500, 1000, 2000))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_smile_lr_fp.csv", "SMILE_LR", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneSmileLassoRegressorDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures")
  val lr = new SmileLassoRegressor().setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, lr))
  /*
  val grid = new ParameterGridBuilder()
    .addGrid(scaler.scalerType, Vector("Raw", "MaxMin", "Standard"))
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(lr.lambda, Vector(3, 1, 0.1, 0.03, 0.01))
    .addGrid(lr.maxIter, Vector(500, 1000, 2000))
    .build()
    */
  val grid = new ParameterGridBuilder()
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(lr.lambda, Vector(3.0, 1.0))
    .addGrid(lr.maxIter, Vector(10, 100))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_lr_desc.csv", "SMILE_LR_DESC", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)

}