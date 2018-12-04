package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{HasFeaturesAndLabelParameters, HasParameters, Parameter}
import libsvm._

// parameters to share between estimator and model

trait HasSvmParameters extends HasParameters {

  val degree = new Parameter[Int](this, "degree", "Degree for polynomial or tanimoto kernel")

  def setDegree(value: Int): this.type = setParameter(degree, value)

  setDefaultParameter(degree, 3)

  private[estimator] var svmModel: Option[svm_model] = None
  private[estimator] var numFeatures: Option[Int] = None
  private[estimator] var svFeatures: Option[Vector[VectorBase]] = None
  private[estimator] var kernelFunction: Option[(VectorBase, VectorBase) => Double] = None

}

case class SmvModelInformation(numFeatures: Int, svmModel: svm_model, trainingFeatures: Option[Vector[VectorBase]] = None, kernelFunction: Option[(VectorBase, VectorBase) => Double] = None)

trait LibSvmEstimator extends HasParameters with HasFeaturesAndLabelParameters with HasSvmParameters {
  // see https://www.csie.ntu.edu.tw/~r94100/libsvm-2.8/README for an of kernels and parameters

  val kernelType = new Parameter[String](this, "kernelType", "The type of SVM kernel to use (LINEAR, POLY, RBF, SIGMOID, TANIMOTO)")

  def setKernelType(value: String): this.type = setParameter(kernelType, value)

  setDefaultParameter(kernelType, "RBF")

  val gamma = new Parameter[Double](this, "gamma", "Gamma term in all non-linear kernels (best guess = 1/nFeatures)")

  def setGamma(value: Double): this.type = setParameter(gamma, value)

  setDefaultParameter(gamma, 0.01)

  val coef0 = new Parameter[Double](this, "coef0", "Coef0 term in polynomial and sigmoid kernels")

  def setCoef0(value: Double): this.type = setParameter(coef0, value)

  setDefaultParameter(coef0, 0.0)

  val C = new Parameter[Double](this, "C", "Soft/hard margin classification term- set low to regularize")

  def setC(value: Double): this.type = setParameter(C, value)

  setDefaultParameter(C, 1.0)

  val eps = new Parameter[Double](this, "eps", "Epsilon term for stopping criterion tolerance")

  def setEps(value: Double): this.type = setParameter(eps, value)

  setDefaultParameter(eps, 0.01)

  val svmOutput = new Parameter[Boolean](this, "svmOutput", "Set true to print output from libSVM to console")

  def setSvmOutput(value: Boolean): this.type = setParameter(svmOutput, value)

  setDefaultParameter(svmOutput, true)

  val maxIterations = new Parameter[Int](this, "maxIterations", "The maximum number of iterations (default 1000- set to -1 for libSVM default)")

  def setMaxIterations(value: Int): this.type  = setParameter(maxIterations, value)

  setDefaultParameter(maxIterations, 2000)

  private class SvmPrinter extends svm_print_interface {
    def print(s: String): Unit = {}
  }

  protected def buildProblem(labels: Vector[Double], features: Vector[VectorBase]): svm_problem = {
    require(labels.length == features.length)

    val numInstances = labels.length
    val noFeatures = features(0).length

    val svmProblem = new svm_problem()
    svmProblem.l = numInstances
    svmProblem.y = new Array[Double](numInstances)
    svmProblem.x = Array.ofDim[svm_node](numInstances, noFeatures)
    var instanceNo = 0
    labels.zip(features).foreach { case (label: Double, instanceFeatures: VectorBase) =>

      instanceFeatures.toArray.zipWithIndex.foreach { case (feature, featureIndex) =>
        val svmNode = new svm_node()
        svmNode.index = featureIndex + 1
        svmNode.value = feature
        svmProblem.x(instanceNo)(featureIndex) = svmNode
      }
      svmProblem.y(instanceNo) = label
      instanceNo += 1
    }

    numFeatures = Some(noFeatures)

    if (!getParameter(svmOutput)) {
      svm.svm_set_print_string_function(new SvmPrinter)
    }

    svmProblem
  }

  protected def buildTanimotoKernel(labels: Vector[Double], features: Vector[VectorBase]): svm_problem = {
    val deg = getParameter(degree)
    val c = getParameter(coef0)
    val g = getParameter(gamma)

    val tanimotoFn = (a: VectorBase, b: VectorBase) => {
      val t = a.tanimoto(b)
      assert(t >= 0 && t <= 1.0)
      Math.pow(g * t + c, deg)
    }

    buildCustomProblem(labels, features, tanimotoFn)
  }


  protected def buildCustomProblem(labels: Vector[Double], features: Vector[VectorBase], kernelFunction: (VectorBase, VectorBase) => Double): svm_problem = {

    require(labels.length == features.length)

    val numInstances = labels.length

    val svmProblem = new svm_problem()
    svmProblem.l = numInstances
    svmProblem.y = new Array[Double](numInstances)
    svmProblem.x = Array.ofDim[svm_node](numInstances, numInstances + 1)

    labels.zip(features).zipWithIndex.foreach { case ((label: Double, instanceFeatures: VectorBase), instanceNo) =>

      var svmNode = new svm_node()
      svmNode.index = 0
      svmNode.value = instanceNo + 1
      svmProblem.x(instanceNo)(0) = svmNode
      svmProblem.y(instanceNo) = label

      features.zipWithIndex.foreach { case (otherFeatures, otherIndex) =>
        svmNode = new svm_node()
        svmNode.index = otherIndex + 1
        svmNode.value = kernelFunction(instanceFeatures, otherFeatures)
        svmProblem.x(instanceNo)(otherIndex + 1) = svmNode
      }

    }

    numFeatures = Some(numInstances + 1)

    svFeatures = Some(features)

    this.kernelFunction = Some(kernelFunction)

    if (!getParameter(svmOutput)) {
      svm.svm_set_print_string_function(new SvmPrinter)
    }

    svmProblem

  }

  protected def buildParams(): svm_parameter = {
    val param = new svm_parameter()
    param.kernel_type = getParameter(kernelType).toUpperCase match {
      case "LINEAR" => svm_parameter.LINEAR
      case "POLY" => svm_parameter.POLY
      case "RBF" => svm_parameter.RBF
      case "SIGMOID" => svm_parameter.SIGMOID
      case "TANIMOTO" => svm_parameter.PRECOMPUTED
      case _ => throw new IllegalArgumentException(s"Unknown kernel type ${getParameter(kernelType)}")
    }

    param.degree = getParameter(degree)
    param.gamma = getParameter(gamma) // in scikit learn default value of gamma is 1/num_features (for RBF)
    param.coef0 = getParameter(coef0)
    param.C = getParameter(C)
    param.eps = getParameter(eps)
    param.max_iter = getParameter(maxIterations)

    param.nu = 0.5 // NA
    param.p = 0.1 // NA

    param.cache_size = 200 // cache size in MB
    // not sure to use shrinking 0 or 1
    param.shrinking = 0
    param.probability = 1
    param.nr_weight = 0
    param.weight_label = new Array[Int](0)
    param.weight = new Array[Double](0)
    param
  }

  protected def buildModel(param: svm_parameter, dataTable: DataTable): Unit = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)

    val svmProblem = getParameter(kernelType).toUpperCase() match {
      case "TANIMOTO" => buildTanimotoKernel(labels, features)
      case _ => buildProblem(labels, features)
    }
    val check = svm.svm_check_parameter(svmProblem, param)
    if (check != null) {
      throw new IllegalArgumentException(s"SVM parameter error: ${check}")
    }
    val model = svm.svm_train(svmProblem, param)
    svmModel = Some(model)
  }

  protected def copyToModel(model: LibSvmModel): model.type = {
    copyParameterValues(model)
    model.svmModel = svmModel
    model.numFeatures = numFeatures
    model.svFeatures = svFeatures
    model.kernelFunction = kernelFunction
    model
  }
}

trait LibSvmModel extends HasSvmParameters {

  lazy val predictNodes: Array[svm_node] = {
    val nodes = new Array[svm_node](numFeatures.get)
    nodes.indices.foreach { index =>
      nodes(index) = new svm_node()
      nodes(index).index = index + 1
    }
    nodes
  }

  protected def buildNodes(testFeatures: VectorBase): Unit = {

    if (kernelFunction != None) {
      svmModel.get.SV.foreach { n =>
        val index = (n(0).value + 0.0001).toInt - 1
        predictNodes(index + 1).value = kernelFunction.get(testFeatures, svFeatures.get(index))
      }
    } else {
      testFeatures.toArray.zipWithIndex.foreach { case (v, index) =>
        predictNodes(index).value = v
      }
    }
  }
}