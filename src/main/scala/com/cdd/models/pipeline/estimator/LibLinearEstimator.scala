package com.cdd.models.pipeline.estimator

import java.io.PrintStream

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{HasFeaturesAndLabelParameters, Parameter}
import de.bwaldvogel.liblinear.{Model => LinearModel, Parameter => LibLinearParameter}
import de.bwaldvogel.liblinear._
import org.apache.commons.io.output.NullOutputStream

trait LibLinearEstimator extends HasSvmParameters with HasFeaturesAndLabelParameters {

  val C = new Parameter[Double](this, "C", "Soft/hard margin classification term- set low to regularize")

  def setC(value: Double): this.type = setParameter(C, value)

  setDefaultParameter(C, 1.0)

  val eps = new Parameter[Double](this, "eps", "Epsilon term for stopping criterion tolerance")

  def setEps(value: Double): this.type = setParameter(eps, value)

  setDefaultParameter(eps, 0.001)

  val svmOutput = new Parameter[Boolean](this, "svmOutput", "Set true to print output from libSVM to console")

  def setSvmOutput(value: Boolean): this.type = setParameter(svmOutput, value)

  setDefaultParameter(svmOutput, true)

  val regularizationType = new Parameter[String](this, "regularizationType", "Regularization type (L1 or L2)")

  def setRegularizationType(value: String): this.type = setParameter(regularizationType, value)

  setDefaultParameter(regularizationType, "L2")

  val maxIterations = new Parameter[Int](this, "maxIterations", "The maximum number of iterations (default 1000- set to -1 for libSVM default)")

  def setMaxIterations(value: Int): this.type  = setParameter(maxIterations, value)

  setDefaultParameter(maxIterations, 2000)

  protected def train(regression: Boolean, dataTable: DataTable): LinearModel = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)
    val numInstances = labels.length

    val problem = new Problem
    val numFeatures = features(0).length()
    problem.l = numInstances
    problem.n = numFeatures + 1
    problem.y = new Array[Double](numInstances)
    problem.x = Array.ofDim[Feature](numInstances, numFeatures)

    labels.zip(features).zipWithIndex.foreach { case ((label: Double, instanceFeatures: VectorBase), instanceNo) =>

      instanceFeatures.toArray.zipWithIndex.foreach { case (feature, featureIndex) =>
        problem.x(instanceNo)(featureIndex) = new FeatureNode(featureIndex + 1, feature)
      }

      problem.y(instanceNo) = label
    }

    val solverType = getParameter(regularizationType) match {
      case "L1" =>
        if (regression)
          throw new IllegalArgumentException("L1 regularization is not available for regression")
        else
          SolverType.L1R_L2LOSS_SVC
      case "L2" => if (regression) SolverType.L2R_L2LOSS_SVR else SolverType.L2R_L2LOSS_SVC
      case _ => throw new IllegalArgumentException("Unknown regularization")
    }
    val param = new LibLinearParameter(solverType, getParameter(C), getParameter(maxIterations), getParameter(eps))

    if (! getParameter(svmOutput)) {
      Linear.setDebugOutput(new PrintStream(new NullOutputStream))
    }

    Linear.train(problem, param)
  }

}

trait LibLinearModel {
  var featureArrayOption:Option[Array[Feature]] = None

  protected def encodeFeatures(features: VectorBase): Array[Feature] = {
    if (featureArrayOption == None)
      featureArrayOption = Some(new Array[Feature](features.length()))
    val featureArray = featureArrayOption.get
    features.toArray().zipWithIndex.foreach { case (feature, index) =>
      featureArray(index) = new FeatureNode(index + 1, feature)
    }
    featureArray
  }
}
