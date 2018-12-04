package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.{SmileRandomForestRegressor, WekaRandomForestRegressor}
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.utils.TestSystems.TestSystemDir
import com.cdd.models.validation.Tuner._

object TuneWekaRandomForestRegressor extends App {
  val filter = new FilterSameFeatureValues()
  val rf = new WekaRandomForestRegressor().setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))

    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_weka_rfr.csv", "WEKA_RFR", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneWekaRandomForestRegressorWithFolding extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val rf = new WekaRandomForestRegressor().setFeaturesColumn("foldedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))

    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_weka_fold_rfr.csv", "WEKA_FOLD_RFR", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneSmileRandomForestRegressorFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val rf = new SmileRandomForestRegressor().setFeaturesColumn("foldedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rf.maxNodes, Vector(1000, 5000, 10000))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_smile_rfr_fp.csv", "SMILE_RFR_FP", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneSmileRandomForestRegressorDesc extends App {
  val filter = new FilterSameFeatureValues()
  val rf = new SmileRandomForestRegressor().setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(rf.maxNodes, Vector(1000, 5000, 10000))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator)
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_rfr_desc.csv", "SMILE_RFR_DESC", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}
