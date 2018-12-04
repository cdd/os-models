package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import smile.regression.LASSO

/**
  * A class that uses the SMILE toolkit implementation to perform LASSO (L1) regression
  * @param uid
  */
class SmileLassoRegressor(val uid:String) extends Predictor[SmileLassoRegressionModel] with HasFeaturesAndLabelParameters {
  def this() = this(Identifiable.randomUID("smile_lasso"))

  val lambda = new Parameter[Double](this, "lambda", "lambda parameter for L1 regularization")
  def setLambda(value:Double): this.type = setParameter(lambda, value)
  setDefaultParameter(lambda, 0.1)

  val tolerance = new Parameter[Double](this, "tolerance", "convergence parameter for LASSO regression")
  def setTolerance(value:Double): this.type = setParameter(tolerance, value)
  setDefaultParameter(tolerance, 1e-4)

  val maxIter = new Parameter[Int](this, "maxIter", "maximum number of iterations")
  def setMaxIter(value:Int): this.type = setParameter(maxIter, value)
  setDefaultParameter(maxIter, 1000)

  override def fit(dataTable: DataTable): SmileLassoRegressionModel = {
    val (labels, features) = extractFeaturesAndLabelsArrays(dataTable)
    val lasso = new LASSO(features, labels, getParameter(lambda), getParameter(tolerance), getParameter(maxIter))
    copyParameterValues(new SmileLassoRegressionModel(uid, lasso))
  }
}

/**
  * A model for SMILE LASSO regression
  *
  * @param uid
  * @param lasso
  */
class SmileLassoRegressionModel(val uid:String, val lasso:LASSO) extends PredictionModel[SmileLassoRegressionModel] {
  override def transformRow(features: VectorBase): Double = {
    lasso.predict(features.toArray())
  }
}
