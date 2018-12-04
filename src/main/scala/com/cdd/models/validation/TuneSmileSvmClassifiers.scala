package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{SmileSvmClassifier, SvmClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.TestSystems.TestSystemDir
import com.cdd.models.validation.Tuner._

object TuneSmileSvmClassifierTanimoto extends App {
  val filter = new FilterSameFeatureValues()
  val svm = new SmileSvmClassifier().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, svm))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(svm.coef0, Vector(0.0, 1.0, 2.0, 3.0))
    .addGrid(svm.degree, Vector(2, 3, 4, 5))
    .addGrid(svm.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp")
  runTuning("tune_smile_svc_tanimoto_fp.csv", "SMILE_SVC_TAN", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSmileSvmClassifierRbf extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmClassifier().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_svc_rbf.csv", "SMILE_SVC_RBF", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSmileSvmClassifierPolynomial extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  // setting degree higher than 2 results in failure to converge for many systems in libSvm, not sure about smile
  val svr = new SmileSvmClassifier().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
    .addGrid(svr.degree, Vector(2, 3, 4))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_svc_poly.csv", "SMILE_SVC_POLY", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSmileClassifierLinear extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmClassifier().setKernelType("Linear").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs("no" -> "no", "activity_value" -> "label",
    "fingerprints_RDKit_FCFP6" -> "rdkit_fp", "fingerprints_CDK_FCFP6" -> "cdk_fp",
    "cdk_descriptors" -> "cdk_desc", "rdkit_descriptors" -> "rdkit_desc")
  runTuning("tune_smile_svc_lin.csv", "SMILE_SVC_LIN", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}
