package com.cdd.spark.pipeline.estimator

import libsvm.{svm_node, svm_parameter, svm_problem}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.Row

/**
  * Created by gjones on 6/13/17.
  */
trait LibSvmEstimator extends Params {
   // see https://www.csie.ntu.edu.tw/~r94100/libsvm-2.8/README for an of kernels and parameters

  val kernelType = new Param[String](this, "kernelType", "The type of SVM kernel to use (LINEAR, POLY, RBF, SIGMOID")

  def setKernelType(value: String): this.type = set(kernelType, value)

  setDefault(kernelType, "RBF")

  val degree = new Param[Int](this, "degree", "Degree for polynomial kernel")

  def setDegree(value: Int): this.type = set(degree, value)

  setDefault(degree, 3)

  val gamma = new Param[Double](this, "gamma", "Gamma term in all non-linear kernels (best guess = 1/nFeatures)")

  def setGamma(value: Double): this.type = set(gamma, value)

  setDefault(gamma, 0.01)

  val coef0 = new Param[Double](this, "coef0", "Coef0 term in polynomial and sigmod kernels")

  def setCoef0(value: Double): this.type = set(coef0, value)

  setDefault(coef0, 0.0)

  val C = new Param[Double](this, "C", "Soft/hard margin classification term- set low to regularize")

  def setC(value: Double): this.type = set(C, value)

  setDefault(C, 1.0)

  val eps = new Param[Double](this, "eps", "Epsilon term for stopping criterion tolerance")

  def setEps(value: Double): this.type = set(eps, value)

  setDefault(eps, 0.01)

  var numFeatures:Option[Int] = None

  val forceCoalesce = new Param[Boolean](this, "forceCoalesce", "Set true to coalesce input dataframe to single partition")

  setDefault(forceCoalesce, false)

  def setForceCoalesce(value: Boolean): this.type = set(forceCoalesce, value)

  protected def buildProblem(data: Array[(Double, Vector)]): svm_problem = {
    val numInstances = data.length
    val noFeatures = data(0)._2.size

    val svmProblem = new svm_problem()
    svmProblem.l = numInstances
    svmProblem.y = new Array[Double](numInstances)
    svmProblem.x = Array.ofDim[svm_node](numInstances, noFeatures)

    var instanceNo = 0
    data.foreach { case (label: Double, features: Vector) =>

      features.toArray.zipWithIndex.foreach { case (feature, featureIndex) =>
        val svmNode = new svm_node()
        svmNode.index = featureIndex + 1
        svmNode.value = feature
        svmProblem.x(instanceNo)(featureIndex) = svmNode
        svmProblem.y(instanceNo) = label
      }
      instanceNo += 1
    }

    this.numFeatures = Some(noFeatures)
    svmProblem
  }

  protected def buildParams(): svm_parameter = {
    val param = new svm_parameter()
    param.kernel_type = $(kernelType).toUpperCase match {
      case "LINEAR" => svm_parameter.LINEAR
      case "POLY" => svm_parameter.POLY
      case "RBF" => svm_parameter.RBF
      case "SIGMOID" => svm_parameter.SIGMOID
      case _ => throw new IllegalArgumentException(s"Unknown kernel type ${$(kernelType)}")
    }
    param.degree = $(degree)
    param.gamma = $(gamma)
    param.coef0 = $(coef0)
    param.C = $(C)
    param.eps = $(eps)

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
}

trait LibSvmModel {
   val predictNodes:Array[svm_node]  = {
    val nodes = new Array[svm_node](numFeatures)
    (0 until nodes.length).foreach { index =>
      nodes(index) = new svm_node()
      nodes(index).index = index + 1
    }
    nodes
  }
  def numFeatures: Int
}