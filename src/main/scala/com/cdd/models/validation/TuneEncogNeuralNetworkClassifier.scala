package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{EpochNeuralNetworkClassifier, LibLinearClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.TestSystems.TestSystemDir
import com.cdd.models.validation.Tuner._

object TuneEncogNeuralNetworkClassifierFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures").setOutputColumn("foldedFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  val nnc = new EpochNeuralNetworkClassifier().setFeaturesColumn("normalizedFeatures").setNoEpochs(100)
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, nnc))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(folder.foldSize, Vector(1000, 1500, 2000))
    .addGrid(nnc.lamdba, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(nnc.layers, Vector(Vector(50), Vector(75), Vector(100)))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_nnc_fp.csv", "SVC_NNC_FP", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneEncogNeuralNetworkClassifierDesc extends App {
  val filter = new FilterSameFeatureValues().setOutputColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setOutputColumn("normalizedFeatures").setFeaturesColumn("uniqueFeatures")
  val nnc = new EpochNeuralNetworkClassifier().setFeaturesColumn("normalizedFeatures").setNoEpochs(100)
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, nnc))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(nnc.lamdba, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(nnc.layers, Vector(Vector(50), Vector(75), Vector(100)))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_nnc_desc.csv", "SVC_NNC_DESC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}
