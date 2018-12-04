package com.cdd.models.tox.tuning

import java.io.File

import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{SmileNaiveBayesClassifier, SmileSvmClassifier, WekaRandomForestClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGrid, ParameterGridBuilder}
import com.cdd.models.tox.Tox21ClassificationModel
import com.cdd.models.utils.{HasLogging, Util}

object Tox21Tune {
  val tuningDir = Util.getProjectFilePath("data/Tox21/tuning")

  def resultsFile(toxField:String, estimatorName:String):File =
    new File(tuningDir, s"tune_${toxField}_${estimatorName}.csv".replace(' ', '_'))
}

class Tox21Tune(val toxField: String, val methodName: String, val optimizer: HyperparameterOptimizer) extends HasLogging {

  def this(toxField: String, optimizer: HyperparameterOptimizer) =
    this(toxField, optimizer.getParameter(optimizer.estimator).getEstimatorName(), optimizer)

  import Tox21Tune._

  def runTuning():Unit = {
    val resultsFile = Tox21Tune.resultsFile(toxField, methodName)
    if (resultsFile.exists()) {
      logger.info(s"Skipping tuning for $toxField $methodName: results file already exists")
      return
    }
    val dataTable = Tox21ClassificationModel.inputTable(toxField)
    logger.warn(s"Running CV on field $toxField method $methodName")
    val results = optimizer.optimizeParameters(dataTable)
    results.logToFile(resultsFile.getAbsolutePath, toxField, methodName)
  }
}

trait OptimizerApp extends App {

  def runOptimizer(toxField: String, pipeline: Pipeline, grid: ParameterGrid): Unit = {
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
      .setNumFolds(3).setParameterGrid(grid)
    val tuner = new Tox21Tune(toxField, optimizer)
    tuner.runTuning()
  }
}

object OptimizeWekaRfClassification extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
  val rf = new WekaRandomForestClassifier()
    .setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, rf))
    .setEstimatorName("Weka RF classification")
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()

  runOptimizer(args(0), pipeline, grid)
}

object OptimizeSmileSvmClassifierTanimoto extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
  val svm = new SmileSvmClassifier()
    .setKernelType("TANIMOTO")
    .setGamma(1.0)
    .setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, svm))
    .setEstimatorName("Smile SVC Tanimoto classification")
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(svm.coef0, Vector(0.0, 1.0, 2.0, 3.0))
    .addGrid(svm.degree, Vector(2, 3, 4, 5))
    .addGrid(svm.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .build()

  runOptimizer(args(0), pipeline, grid)
}

object OptimizeSmileSvmClassifierPoly extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler()
    .setFeaturesColumn("uniqueFeatures")
    .setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmClassifier()
    .setKernelType("POLY")
    .setGamma(1.0)
    .setFeaturesColumn("normalizedFeatures")
    .setMaxIter(10000)
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
    .setEstimatorName("Smile SVC Poly classification")
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .addGrid(svr.coef0, Vector(0.0, 2.0, 4.0, 6.0))
    .addGrid(svr.degree, Vector(2, 3, 4))
    .build()

  runOptimizer(args(0), pipeline, grid)
}

object OptimizeSmileSvmClassifierRbf extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
  val scaler = new FeatureScaler()
    .setFeaturesColumn("uniqueFeatures")
    .setOutputColumn("normalizedFeatures")
  val svr = new SmileSvmClassifier()
    .setKernelType("RBF")
    .setFeaturesColumn("normalizedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, scaler, svr))
    .setEstimatorName("Smile SVC RBF classification")
  val grid = new ParameterGridBuilder()
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp", "rdkit_desc", "cdk_desc"))
    .addGrid(scaler.scalerType, Vector("MaxMin", "Standard"))
    .addGrid(svr.C, Vector(0.001, 0.01, 0.1, 1, 10))
    .addGrid(svr.gamma, Vector(0.0001, 0.001, 0.01, 0.1, 1))
    .build()

  runOptimizer(args(0), pipeline, grid)
}

object OptimizeWekaRfClassificationFoldedFp extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
    .setFeaturesColumn("cdk_fp")
  val folder = new VectorFolder()
    .setFeaturesColumn("uniqueFeatures")
  val rf = new WekaRandomForestClassifier()
    .setFeaturesColumn("foldedFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, folder, rf))
    .setEstimatorName("Weka RF on folded fp classification")
  val grid = new ParameterGridBuilder()
    .addGrid(folder.foldSize, Vector(500, 1000, 2000))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(rf.maxDepth, Vector(15, 20, 30, 40, 50))
    .addGrid(rf.numTrees, Vector(30, 40, 50, 60, 70))
    .build()

  runOptimizer(args(0), pipeline, grid)
}

object OptimizeSmileNaiveBayesClassifier extends OptimizerApp {
  val filter = new FilterSameFeatureValues()
  val nb = new SmileNaiveBayesClassifier()
    .setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, nb)).setEstimatorName("Smile Bernoulli NB classification")
  val grid = new ParameterGridBuilder()
    //.addGrid(binarizer.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(nb.method, Vector("Bernoulli", "Multinomial"))
    .addGrid(nb.alpha, Vector(0.0, 0.5, 1.0, 2.0))
    .build()

  runOptimizer(args(0), pipeline, grid)
}
