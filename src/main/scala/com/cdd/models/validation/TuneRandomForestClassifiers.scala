package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{SmileRandomForestClassifier, WekaRandomForestClassifier}
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import Tuner._
import com.cdd.models.utils.TestSystems.TestSystemDir

object TuneWekaRandomForestClassifier extends App {
  val filter = new FilterSameFeatureValues()
  val rf = new WekaRandomForestClassifier().setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"),
    ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"),
    ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_weka_rfc.csv", "WEKA_RFC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneWekaRandomForestClassifierWithFolding extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val rf = new WekaRandomForestClassifier().setFeaturesColumn("foldedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"),
    ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_weka_fold_rfc.csv", "WEKA_FOLD_RFC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSmileRandomForestClassifierFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val rf = new SmileRandomForestClassifier().setFeaturesColumn("foldedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rf.maxNodes, Vector(1000, 5000, 10000))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"),
    ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_smile_rfc_fp.csv", "SMILE_RFC_FP", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}



object TuneSmileRandomForestClassifierDesc extends App {
  val filter = new FilterSameFeatureValues()
  val rf = new SmileRandomForestClassifier().setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, rf))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(rf.maxNodes, Vector(1000, 5000, 10000))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"),
    ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_smile_rfc_desc.csv", "SMILE_RFC_DESC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}
