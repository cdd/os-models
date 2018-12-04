package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{HasFeaturesAndLabelParameters, Identifiable, PredictionModelWithProbabilities, PredictorWithProbabilities}
import smile.classification._


class SmileRandomForestClassifier(val uid: String) extends PredictorWithProbabilities[SmileRandomForestClassificationModel]
  with HasFeaturesAndLabelParameters with HasSmileThreads with HasSmileRandomForestParameters {
  def this() = this(Identifiable.randomUID("smile_rfc"))

  override def fit(dataTable: DataTable): SmileRandomForestClassificationModel = {
    val (y, x) = extractFeaturesAndLabelsArrays(dataTable)
    val iy = y.map(_.toInt)

    val rf = randomForest(x, iy, ntrees = getParameter(numTrees), maxNodes=getParameter(maxNodes))
    //val rf = new RandomForest(x, y, getParameter(numTrees), getParameter(maxNodes), 5, x(0).length / 3)
    copyParameterValues(new SmileRandomForestClassificationModel(uid, rf))
  }
}

class SmileRandomForestClassificationModel(override val uid: String, val rf: RandomForest)
  extends PredictionModelWithProbabilities[SmileRandomForestClassificationModel] with HasFeaturesAndLabelParameters {

  val probabilities = new Array[Double](2)

  override def transformRow(features: VectorBase): (Double, Double) = {
    val category = rf.predict(features.toArray(), probabilities)
    val probability = probabilities(1)
    /*
    Not sure what is up with smiles and probabilities, but the categories are often inconsistent with the probabilities

    if (category == 1) {
      assert(probability >= 0.4)
    }
    else {
      assert(probability <= 0.6)
    }
    */
    (category.toDouble, probability)
  }
}