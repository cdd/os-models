package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{Identifiable, PredictionModel, Predictor, PredictorWithProbabilities}
import de.bwaldvogel.liblinear.{Linear, Model => LinearModel}

class LibLinearRegressor(val uid: String) extends Predictor[LibLinearRegressionModel] with LibLinearEstimator {

  def this() = this(Identifiable.randomUID("linear_svr"))

  override def fit(dataTable: DataTable): LibLinearRegressionModel = {
    val linearModel = train(true, dataTable)

    copyParameterValues(new LibLinearRegressionModel(uid, linearModel))
  }
}

class LibLinearRegressionModel(val uid:String, val linearModel: LinearModel)
  extends PredictionModel[LibLinearRegressionModel] with LibLinearModel{

   override def transformRow(features: VectorBase): Double = {
     val linearFeatures = encodeFeatures(features)
     val values = Array(0.0)
     Linear.predictValues(linearModel, linearFeatures, values)
     values(0)
  }
}
