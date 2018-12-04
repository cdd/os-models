package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{Identifiable, PredictionModelWithProbabilities, PredictorWithProbabilities}
import org.encog.neural.networks.BasicNetwork

class EpochNeuralNetworkClassifier(val uid: String) extends PredictorWithProbabilities[EpochNeuralNetworkClassificationModel] with EpochNeuralNetworkEstimator {

  def this() = this(Identifiable.randomUID("enn_classifier"))

  override def fit(dataTable: DataTable): EpochNeuralNetworkClassificationModel = {
    val network = buildNetwork(dataTable, false)

    copyParameterValues(new EpochNeuralNetworkClassificationModel(uid, network, numFeatures.get))
  }
}

class EpochNeuralNetworkClassificationModel(override val uid: String, override val network: BasicNetwork, override val numFeatures: Int)
  extends PredictionModelWithProbabilities[EpochNeuralNetworkClassificationModel] with EpochNeuralNetworkModel {

  override def transformRow(features: VectorBase): (Double, Double) = {
    val raw = compute(features)
    val prediction = if (raw > 0.5) 1.0 else 0.0
    return (prediction, raw)
  }
}