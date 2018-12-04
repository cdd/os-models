package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, DenseVector, SparseVector, VectorBase}
import com.cdd.models.pipeline._
import smile.classification.NaiveBayes.Model
import smile.classification._

class SmileNaiveBayesClassifier(val uid: String)
  extends PredictorWithProbabilities[SmileNaiveBayesClassificationModel]
    with HasFeaturesAndLabelParameters {

  def this() = this(Identifiable.randomUID("smile_nb"))

  val method = new Parameter[String](this, "method", "Naive Bayes method (Bernoulli or Multinomial")

  def setMethod(value: String): this.type = setParameter(method, value)

  setDefaultParameter(method, "Bernoulli")

  val alpha = new Parameter[Double](this, "alpha", "Naive Bayes Laplace smoothing parameter")

  def setAlpha(value: Double): this.type = setParameter(alpha, value)

  setDefaultParameter(alpha, 1.0)

  override def fit(dataTable: DataTable): SmileNaiveBayesClassificationModel = {
    val (numFeatures, labels, denseFeatures, sparseFeatures) = extractFeaturesAndLabelsToSmileArrays(dataTable)
    val ilabels = labels.map(_.toInt)
    val sigma = getParameter(alpha)
    val model = getParameter(method).toLowerCase match {
      case "bernoulli" => Model.BERNOULLI
      case "multinomial" => Model.MULTINOMIAL
      case m => throw new IllegalArgumentException(s"Unknown Bayes model ${m}")
    }
    val nb = if (sparseFeatures == None) {
      naiveBayes(denseFeatures.get, ilabels, model, sigma = sigma)
    } else {
      val nb = new NaiveBayes(model, 2, numFeatures, sigma)
      sparseFeatures.get.zip(ilabels) foreach { case (sa, l) => nb.learn(sa, l) }
      nb
    }

    copyParameterValues(new SmileNaiveBayesClassificationModel(uid, nb))
  }
}

class SmileNaiveBayesClassificationModel(override val uid: String, private val nb: NaiveBayes)
  extends PredictionModelWithProbabilities[SmileNaiveBayesClassificationModel]
    with HasFeaturesAndLabelParameters {

  val probabilities = new Array[Double](2)

  override def transformRow(features: VectorBase): (Double, Double) = {
    val category = features match {
      case sv: SparseVector => nb.predict(SmileUtil.vectorToSparseArray(sv), probabilities)
      case dv: DenseVector => nb.predict(dv.toArray(), probabilities)
    }

    val probability = probabilities(1)
    /*
    if (category == 1) {
      assert(probability >= 0.5)
    }
    else {
      assert(probability <= 1.0)
    }
*/
    (category.toDouble, probability)
  }
}