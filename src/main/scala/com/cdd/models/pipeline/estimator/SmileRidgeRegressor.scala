package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import smile.regression.RidgeRegression

/**
  * A class that uses the SMILE toolkit implementation to perform ridge (L2) regression
  *
  * @param uid
  */
class SmileRidgeRegressor(val uid: String) extends Predictor[SmileRidgeRegressionModel] with HasFeaturesAndLabelParameters {
  def this() = this(Identifiable.randomUID("smile_ridge"))

  val lambda = new Parameter[Double](this, "lambda", "lambda parameter for L1 regularization")

  def setLambda(value: Double): this.type = setParameter(lambda, value)

  setDefaultParameter(lambda, 0.1)

  override def fit(dataTable: DataTable): SmileRidgeRegressionModel = {
  val (labels, features) = extractFeaturesAndLabelsArrays(dataTable)
    val ridge = new RidgeRegression(features, labels, getParameter(lambda))
    copyParameterValues(new SmileRidgeRegressionModel(uid, ridge))
  }
}

/**
  * A model for SMILE ridge regression
  *
  * @param uid
  * @param ridge
  */
class SmileRidgeRegressionModel(val uid: String, val ridge:RidgeRegression) extends PredictionModel[SmileRidgeRegressionModel] with HasFeaturesAndLabelParameters {
  override def transformRow(features: VectorBase): Double = {
    ridge.predict(features.toArray())
  }
}
