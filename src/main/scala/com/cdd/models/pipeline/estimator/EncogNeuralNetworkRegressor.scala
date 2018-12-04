package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{Identifiable, PredictionModel, Predictor}
import org.encog.neural.networks.BasicNetwork

class EpochNeuralNetworkRegressor(val uid: String) extends Predictor[EpochNeuralNetworkRegressionModel]
  with EpochNeuralNetworkEstimator {
  def this() = this(Identifiable.randomUID("enn_regressor"))

  override def fit(dataTable: DataTable): EpochNeuralNetworkRegressionModel = {
    val network = buildNetwork(dataTable, true)

    copyParameterValues(new EpochNeuralNetworkRegressionModel(uid, network, numFeatures.get))
  }
}

class EpochNeuralNetworkRegressionModel(override val uid: String, val network: BasicNetwork, val numFeatures:Int)
  extends PredictionModel[EpochNeuralNetworkRegressionModel] with EpochNeuralNetworkModel {

  override def transformRow(features: VectorBase): Double = {
    return compute(features)
  }
}