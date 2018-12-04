package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import libsvm.{svm, svm_model, svm_parameter}

@SerialVersionUID(3251938127119738013L)
class SvmRegressor(val uid: String) extends Predictor[SvmRegressionModel] with LibSvmEstimator {
  def this() = this(Identifiable.randomUID("svr"))

  val epsilon = new Parameter[Double](this, "epsilon", "set the epsilon in the SVR loss function (set to -1.0 to adjust automatically)")

  def setEpsilon(value: Double): this.type = setParameter(epsilon, value)

 // setDefaultParameter(epsilon, 0.1)
  setDefaultParameter(epsilon, -1.0)

  setDefaultParameter(eps, -1.0)

  override def fit(dataTable: DataTable): SvmRegressionModel = {
    val labelDimensionality = this.labelDimensionality(dataTable)
    var softMargin = getParameter(epsilon)
    if(softMargin == -1.0) {
      softMargin = labelDimensionality/20
      if (softMargin > .1)
        softMargin = 0.1
      else
        logger.info(s"Adjusting soft margin parameter to $softMargin")
    }
    var tol = getParameter(this.eps)
    if (tol == -1.0) {
      tol = labelDimensionality * 0.000001
      if (tol > 0.001)
        tol = 0.001
      else
        logger.info(s"Adjusting tolerance parameter to $tol")
    }

    val param = buildParams()
    param.svm_type = svm_parameter.EPSILON_SVR
    param.p = softMargin
    param.probability = 0
    param.eps = tol

    buildModel(param, dataTable)
    copyToModel(new SvmRegressionModel(uid))
  }
}

class SvmRegressionModel(val uid: String)
  extends PredictionModel[SvmRegressionModel] with LibSvmModel{

  override def transformRow(features: VectorBase): Double = {
    buildNodes(features)
    val prediction = svm.svm_predict(svmModel.get, predictNodes)
    prediction
  }

}