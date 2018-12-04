package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import weka.classifiers.bayes.{BayesNet, NaiveBayes, NaiveBayesMultinomial}
import weka.core.Instances

// Weka Naive Bayes does Laplacian smoothing with a value of 1

class WekaNaiveBayesClassifier(val uid: String)
  extends PredictorWithProbabilities[WekaNaiveBayesClassificationModel]
    with HasFeaturesAndLabelParameters {

  def this() = this(Identifiable.randomUID("weka_nbc"))

  // set to true to enable binary nominal features only
  val binary = false

  // Weka Bayes will treat instance values as floating values and apply a normal distribution to determine on or off
  // unless we set them nominal (which is usually what we want)

  // I tried to set number to nominal values to 2, but got (slightly) poorer results.  Unsure, why this would be-
  // I would expect them to be identical

  override def fit(dataTable: DataTable): WekaNaiveBayesClassificationModel = {

    val nb = new NaiveBayes

    val data = extractWekaInstances(dataTable, classification = true, nominalFeaturesCutoff = Int.MaxValue, binary = binary)
    nb.buildClassifier(data)
    val header = new Instances(data, 0)
    copyParameterValues(new WekaNaiveBayesClassificationModel(uid, nb, header, binary))
  }

}

class WekaNaiveBayesClassificationModel(override val uid: String, val nb: NaiveBayes, val header: Instances, binary:Boolean)
  extends PredictionModelWithProbabilities[WekaNaiveBayesClassificationModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): (Double, Double) = {
    WekaUtil.classificationWithProbability(nb, header, features, binary = binary)
  }
}


class WekaMultinomialBayesClassifier(val uid: String)
  extends PredictorWithProbabilities[WekaMultinomialBayesClassificationModel]
    with HasFeaturesAndLabelParameters {

  def this() = this(Identifiable.randomUID("weka_mnbc"))

  override def fit(dataTable: DataTable): WekaMultinomialBayesClassificationModel = {

    val nb = new NaiveBayesMultinomial
    // Weka multinominal bayes requires that attributes types are numeric
    val data = extractWekaInstances(dataTable, classification = true, nominalFeaturesCutoff = -1)
    nb.buildClassifier(data)
    val header = new Instances(data, 0)
    copyParameterValues(new WekaMultinomialBayesClassificationModel(uid, nb, header))
  }
}

class WekaMultinomialBayesClassificationModel(override val uid: String, val nb: NaiveBayesMultinomial, val header: Instances)
  extends PredictionModelWithProbabilities[WekaMultinomialBayesClassificationModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): (Double, Double) = {
    WekaUtil.classificationWithProbability(nb, header, features)
  }
}