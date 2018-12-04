package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.SvmClassifier
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.TestSystems.TestSystemDir
import com.cdd.models.validation.Tuner._

object TuneSvmClassifierTanimoto extends App {
  val filter = new FilterSameFeatureValues()
  val svm = new SvmClassifier().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures")
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

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svc_tanimoto_fp.csv", "SVC_TAN", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSvmClassifierRbfFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmClassifier().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
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
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svc_rbf_fp.csv", "SVC_RBF_FP", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneSvmClassifierRbfDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmClassifier().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_svc_rbf_desc.csv", "SVC_RBF_DESC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}


object TuneSvmClassifierPolynomialFp extends App {
  val filter = new FilterSameFeatureValues()
  val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures")
  val scaler = new FeatureScaler().setFeaturesColumn("foldedFeatures").setOutputColumn("normalizedFeatures")
  // setting degree higher than 2 results in failure to converge for many systems
  val svr = new SvmClassifier().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, scaler, svr))
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(folder.foldSize, Vector(1000, 1500, 2000))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
    .addGrid(svr.degree, Vector(2, 3, 4))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_svc_poly_fp.csv", "SVC_POLY_FP", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}


object TuneSvmClassifierPolynomialDesc extends App {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
  val svr = new SvmClassifier().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
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
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("cdk_descriptors" -> "cdk_desc"), ("rdkit_descriptors" -> "rdkit_desc"))
  runTuning("tune_svc_poly_desc.csv", "SVC_POLY_DESC", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}
