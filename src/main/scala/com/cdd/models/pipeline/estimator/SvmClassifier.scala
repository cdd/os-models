package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import libsvm.{svm, svm_model, svm_parameter}

class SvmClassifier(val uid: String) extends PredictorWithProbabilities[SvmClassificationModel] with LibSvmEstimator {
  def this() = this(Identifiable.randomUID("svc"))

  override def fit(dataTable: DataTable): SvmClassificationModel = {
    val param = buildParams()
    param.svm_type = svm_parameter.C_SVC
    param.probability = 1
    buildModel(param, dataTable)
    copyToModel(new SvmClassificationModel(uid))
  }
}

class SvmClassificationModel(val uid: String)
  extends PredictionModelWithProbabilities[SvmClassificationModel] with LibSvmModel{

  override def transformRow(features: VectorBase): (Double, Double) = {
    val model = svmModel.get
    buildNodes(features)
    val probabilities = new Array[Double](2)
    val prediction = svm.svm_predict_probability(model, predictNodes, probabilities)
    val prob = probabilities(if (model.label(0) == 1) 0 else 1)
    assert (prediction == (if (probabilities(0) >= 0.5) model.label(0) else model.label(1)))
    val test = if (prob >= 0.5) 1.0 else .0
    (prediction, prob)
  }

}
