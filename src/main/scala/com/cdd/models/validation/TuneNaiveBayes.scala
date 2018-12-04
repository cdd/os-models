package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline}
import com.cdd.models.pipeline.estimator.{SmileNaiveBayesClassifier, SvmClassifier, WekaMultinomialBayesClassifier, WekaNaiveBayesClassifier}
import com.cdd.models.pipeline.transformer.{BinarizeTransformer, FilterSameFeatureValues}
import com.cdd.models.pipeline.tuning.{HyperparameterOptimizer, ParameterGridBuilder}
import com.cdd.models.utils.TestSystems.TestSystemDir
import Tuner._

object TuneSmileNaiveBayes extends App {
  val filter = new FilterSameFeatureValues()//.setFeaturesColumn("binaryFeatures")
 // val binarizer = new BinarizeTransformer().setOutputColumn("binaryFeatures")
  val nb = new SmileNaiveBayesClassifier().setFeaturesColumn("uniqueFeatures")
  val pipeline = new Pipeline().setStages(Vector(filter, nb))
  val grid = new ParameterGridBuilder()
    //.addGrid(binarizer.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
    .addGrid(nb.method, Vector("Bernoulli", "Multinomial"))
    .addGrid(nb.alpha, Vector(0.0, 0.5, 1.0, 2.0))
    .build()
  val optimizer = new HyperparameterOptimizer()
    .setEstimator(pipeline)
    .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
    .setNumFolds(3).setParameterGrid(grid)

  val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
  runTuning("tune_smile_nb.csv", "SMILE_NB", optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
}

object TuneWekaNaiveBayesBernoulli extends App {

  tuneNaiveBayes(true, args)

  def tuneNaiveBayes(bernoulli:Boolean, args:Array[String]):Unit = {
    val filter = new FilterSameFeatureValues()
    val nb = if (bernoulli) new WekaNaiveBayesClassifier() else new WekaMultinomialBayesClassifier()
    nb.setFeaturesColumn("uniqueFeatures")
    val pipeline = new Pipeline().setStages(Vector(filter, nb))
    val grid = new ParameterGridBuilder()
      .addGrid(filter.featuresColumn, Vector("rdkit_fp", "cdk_fp"))
      .build()
    val optimizer = new HyperparameterOptimizer()
      .setEstimator(pipeline)
      .setEvaluator(new ClassificationEvaluator().setMetric("rocAuc"))
      .setNumFolds(3).setParameterGrid(grid)

    val mapper = (dt: DataTable) => dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("fingerprints_CDK_FCFP6" -> "cdk_fp"))
    val file = if (bernoulli) "tune_weka_nb_bn.csv" else "tune_weka_nb_mn.csv"
    val name = if (bernoulli) "WEKA_NB_BN" else "WEKA_NB_MN"
    runTuning(file, name, optimizer, mapper, args.toList, TestSystemDir.DISCRETE)
  }
}


object TuneWekaNaiveBayesMultiNominial extends App {
  TuneWekaNaiveBayesBernoulli.tuneNaiveBayes(false, args)
}