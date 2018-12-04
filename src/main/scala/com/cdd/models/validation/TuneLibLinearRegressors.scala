package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.{LibLinearClassifier, LibLinearRegressor}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline, RegressionEvaluator}
import com.cdd.models.utils.TestSystems.TestSystemDir
import com.cdd.models.validation.Tuner._

object TuneLibLinearRegressorFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures").setFoldSize(1000)
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  val svr = new LibLinearRegressor().setFeaturesColumn("normalizedFeatures").setMaxIterations(1000)
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    // for bubonic and kcnq datasets can't use larger fingerprints
    .addGrid(folder.foldSize, Vector(1000, 1500, 2000))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svr_lin_fp.csv", "SVR_LIN_FP", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}

object TuneLinearRegressorDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new LibLinearRegressor().setFeaturesColumn("normalizedFeatures").setMaxIterations(1000)
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new RegressionEvaluator())
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_svr_lin_desc.csv", "SVR_LIN_DESC", optimizer, mapper, args.toList, TestSystemDir.CONTINUOUS)
}
