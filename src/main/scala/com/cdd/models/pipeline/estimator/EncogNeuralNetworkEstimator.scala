package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{HasFeaturesAndLabelParameters, HasParameters, Parameter}
import com.cdd.models.utils.HasLogging
import org.apache.log4j.LogManager
import org.encog.engine.network.activation.{ActivationLinear, ActivationReLU, ActivationSigmoid, ActivationSoftMax}
import org.encog.ml.data.MLDataSet
import org.encog.ml.data.basic.BasicMLDataSet
import org.encog.ml.data.versatile.VersatileMLDataSet
import org.encog.ml.model.EncogModel
import org.encog.neural.networks.BasicNetwork
import org.encog.neural.networks.layers.BasicLayer
import org.encog.neural.networks.training.propagation.resilient.ResilientPropagation

trait EpochNeuralNetworkEstimator
  extends HasParameters with HasFeaturesAndLabelParameters with HasLogging {

  val layers = new Parameter[Vector[Int]](this, "layers", "Number of neurons in hidden layers")

  def setLayers(value: Vector[Int]): this.type = setParameter(layers, value)

  setDefaultParameter(layers, Vector(5, 4))

  val lamdba = new Parameter[Double](this, "lambda", "Regularization parameter")

  def setLamdba(value: Double): this.type = setParameter(lamdba, value)

  setDefaultParameter(lamdba, 0.01)

  val noEpochs = new Parameter[Int](this, "noEpochs", "Number of training epochs")

  def setNoEpochs(value: Int): this.type = setParameter(noEpochs, value)

  setDefaultParameter(noEpochs, 100)

  val minError = new Parameter[Double](this, "minError", "Minimum training error")

  def setMinError(value: Double): this.type = setParameter(minError, value)

  setDefaultParameter(minError, 0.001)

  protected var numFeatures:Option[Int] = None

  protected def buildNetwork(dataTable: DataTable, regression: Boolean): BasicNetwork = {

    val trainingSet = dataTableToMlDataSet(dataTable)
    val noFeatures = trainingSet.getInputSize

    val hasBias = true
    val network = new BasicNetwork
    network.addLayer(new BasicLayer(new ActivationSigmoid, hasBias, noFeatures))
    getParameter(layers).foreach { noNeurons =>
      network.addLayer(new BasicLayer(new ActivationSigmoid, hasBias, noNeurons))
    }
    //network.addLayer(new BasicLayer(new ActivationLinear, hasBias, 1))
    network.addLayer(new BasicLayer(if (regression) new ActivationLinear else  new ActivationSigmoid, hasBias, 1))
    //network.addLayer(new BasicLayer(if (regression) new ActivationLinear else  new ActivationSoftMax, hasBias, 1))
    network.getStructure.finalizeStructure()

    trainingSet.getInputSize
    val trainer = new ResilientPropagation(network, trainingSet)

    var epoch = 0
    var error = .0
    do {
      trainer.iteration()
      epoch += 1
      error = trainer.getError
      logger.debug(s"Epoch $epoch error $error")
    } while (error > getParameter(minError) && epoch < getParameter(noEpochs))

    numFeatures = Some(noFeatures)
    network
  }

  def dataTableToMlDataSet(dataTable: DataTable): MLDataSet = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)

    val noInstances = labels.length
    val noFeatures = features(0).length
    val x = Array.ofDim[Double](noInstances, noFeatures)
    val y = Array.ofDim[Double](noInstances, 1)

    labels.zip(features).zipWithIndex.foreach { case ((label, instanceFeatures), instanceNo) =>

      y(instanceNo)(0) = label

      instanceFeatures match {
        case v: VectorBase =>
          v.toArray().zipWithIndex.foreach { case (feature, featureNo) => x(instanceNo)(featureNo) = feature }
        case _ => throw new IllegalArgumentException("Feature class is not a vector!")
      }
    }
    new BasicMLDataSet(x, y)
  }

  def test(dataTable: DataTable, regression: Boolean): Unit = {

    val trainingSet = dataTableToMlDataSet(dataTable)


  }

}


trait EpochNeuralNetworkModel {
  val network: BasicNetwork
  val numFeatures:Int

  val input = new Array[Double](numFeatures)
  val output = new Array[Double](1)

  def compute(features: VectorBase): Double = {
    features match {
      case v: VectorBase =>
        require(v.length() == numFeatures)
        v.toArray().zipWithIndex.foreach { case (feature, featureNo) => input(featureNo) = feature }
      case _ => throw new IllegalArgumentException("Feature class is not a vector!")
    }

    network.compute(input, output)
    return output(0)
  }
}
