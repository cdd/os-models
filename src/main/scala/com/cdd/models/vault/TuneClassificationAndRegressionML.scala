package com.cdd.models.vault

import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{SmileSvmClassifier, WekaRandomForestClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.{HasLogging, Util}

trait ClassificationAndRegressionMLTuner extends HasLogging {
  private val dataTable = ClassificationAndRegressionML.buildDataTable(true)
  def runTuning(resultsFileName: String, methodName: String, optimizer: HyperparameterOptimizer, bothIsRegression: Boolean = true): Unit = {
    val resultsFile = Util.getProjectFilePath(s"data/vault/protocol_molecule_features/classification_and_regression_ml_tuning/$resultsFileName")
    if (resultsFile.exists())
      require(resultsFile.delete())
    logger.warn(s"Running ML CV on $methodName")
    val results = optimizer.optimizeParameters(dataTable)
    val name = if (bothIsRegression) "BothRegression" else "BothClassification"
    results.logToFile(resultsFile.getAbsolutePath, name, methodName)
  }
}

object TuneClassificationAndRegressionML extends App with ClassificationAndRegressionMLTuner {

  tuneWekaRfClassification()
  tunePolySmileSvmClassifier()
  tuneRbfSmileSvmClassifier()
  tuneLinearSmileSvmClassifier()

  def tuneWekaRfClassification(): Unit = {
    val filter = new FilterSameFeatureValues()
      .setFeaturesColumn("features")
    val rf = new WekaRandomForestClassifier()
      .setFeaturesColumn("uniqueFeatures")
      .setMaxDepth(15)
      .setNumTrees(70)
    val pipeline = new Pipeline().setStages(Vector(filter, rf))
      .setEstimatorName("Weka RF classification")

    val grid = new ParameterGridBuilder()
      .addGrid(rf.maxDepth, Vector(2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100))
      .addGrid(rf.numTrees, Vector(20, 30, 40, 50, 60, 70, 80, 90, 100))
      .build()
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("f1"))
      .setNumFolds(3).setParameterGrid(grid)
    runTuning("tune_weka_rf_ml_classification.csv", "WEKA_RF", optimizer)
  }

  def tunePolySmileSvmClassifier(): Unit = {
    val filter = new FilterSameFeatureValues()
      .setFeaturesColumn("features")
    val scaler = new FeatureScaler()
      .setFeaturesColumn("uniqueFeatures")
      .setOutputColumn("normalizedFeatures")
      .setScalerType("MaxMin")
    val svr = new SmileSvmClassifier().setKernelType("POLY").setGamma(1.0).setFeaturesColumn("normalizedFeatures")
    val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
    val grid = new ParameterGridBuilder()
      .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
      .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
      .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
      .addGrid(svr.degree, Vector(2, 3, 4))
      .build()
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("f1"))
      .setNumFolds(3).setParameterGrid(grid)
    runTuning("tune_smile_poly_svm_ml_classification.csv", "SMILE_POLY_SVM", optimizer)
  }

  def tuneRbfSmileSvmClassifier(): Unit = {
    val filter = new FilterSameFeatureValues()
      .setFeaturesColumn("features")
    val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
    val svr = new SmileSvmClassifier().setKernelType("RBF").setFeaturesColumn("normalizedFeatures")
    val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
    val grid = new ParameterGridBuilder()
      .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
      .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10, 100, 1000))//, 10000 , 1000000))
      .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
      .build()
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("f1"))
      .setNumFolds(3).setParameterGrid(grid)
    runTuning("tune_smile_rbf_svm_ml_classification.csv", "SMILE_RBF_SVM", optimizer)
  }

  def tuneLinearSmileSvmClassifier(): Unit = {
    val filter = new FilterSameFeatureValues()
      .setFeaturesColumn("features")
    val scaler = new FeatureScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedFeatures")
    val svr = new SmileSvmClassifier().setKernelType("Linear").setFeaturesColumn("normalizedFeatures")
    val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
    val grid = new ParameterGridBuilder()
      .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
      .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1, 10, 100))
      .build()
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("f1"))
      .setNumFolds(3).setParameterGrid(grid)
    runTuning("tune_smile_linear_svm_ml_classification.csv", "SMILE_LINEAR_SVM", optimizer)
  }

  def tuneMajorityVotingClassifier(): Unit = {

  }
}