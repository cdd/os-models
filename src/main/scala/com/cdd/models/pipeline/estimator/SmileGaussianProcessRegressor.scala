package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import smile.math.SparseArray
import smile.regression._

trait HasSmileGaussianProcessParameters extends HasParameters with SmileMercerKernel with HasFeaturesAndLabelParameters {

  val lambda = new Parameter[Double](this, "lambda", "shrinkage/regularization parameter")
  def setLambda(value:Double): this.type=setParameter(lambda, value)
  setDefaultParameter(lambda, 1.0)

}

class SmileGaussianProcessRegressor(val uid: String) extends Predictor[SmileGaussianProcessRegressionModel]
  with HasSmileGaussianProcessParameters {
  def this() = this(Identifiable.randomUID("smile_gpr"))

  override def fit(dataTable: DataTable): SmileGaussianProcessRegressionModel = {

    val (numFeatures, labels, denseFeatures, sparseFeatures) = extractFeaturesAndLabelsToSmileArrays(dataTable)
    var sparseGpr:Option[GaussianProcessRegression[SparseArray]] = None
    var denseGpr:Option[GaussianProcessRegression[Array[Double]]] = None

    if (sparseFeatures != None) {
      val kernel = sparseKernel()
      sparseGpr = Some(new GaussianProcessRegression[SparseArray](sparseFeatures.get.toArray, labels, kernel, getParameter(lambda)))
    } else {
      val kernel = denseKernel()
      denseGpr = Some(new GaussianProcessRegression[Array[Double]](denseFeatures.get, labels, kernel, getParameter(lambda)))
    }

    copyParameterValues(new SmileGaussianProcessRegressionModel(uid, sparseGpr, denseGpr))
  }
}

class SmileGaussianProcessRegressionModel(override val uid: String,
                                          sparseGpr:Option[GaussianProcessRegression[SparseArray]],
                                          denseGpr:Option[GaussianProcessRegression[Array[Double]]])
  extends PredictionModel[SmileGaussianProcessRegressionModel] with HasSmileGaussianProcessParameters {

  override def transformRow(features: VectorBase): Double = {
    if (sparseGpr != None) {
      sparseGpr.get.predict(SmileUtil.vectorToSparseArray(features))
    } else {
      denseGpr.get.predict(features.toArray())
    }
  }
}
