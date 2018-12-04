
package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._

trait HasRegressorTemplateParameters extends HasParameters {

}

class RegressorTemplateRegressor (val uid: String) extends Predictor[RegressorTemplateRegressionModel] with HasFeaturesAndLabelParameters with HasRegressorTemplateParameters {
  def this() = this(Identifiable.randomUID("weka_rfr"))

  override def fit(dataTable: DataTable): RegressorTemplateRegressionModel = {
     null
  }
}

class RegressorTemplateRegressionModel (override val uid: String)
  extends PredictionModel[RegressorTemplateRegressionModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): Double = {
    -1.0
  }
}