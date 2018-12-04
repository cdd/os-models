package com.cdd.models.pipeline.estimator

import com.cdd.models.pipeline._
import smile.math.SparseArray
import smile.math.kernel._

trait SmileMercerKernel extends HasParameters {

  val kernelType = new Parameter[String](this, "kernelType", "Mercer kernel for SVM/GPR [One of Linear, Poly, Gaussian, Tanimoto")

  def setKernelType(value: String): this.type = setParameter(kernelType, value)

  setDefaultParameter(kernelType, "Poly")

  val degree = new Parameter[Int](this, "degree", "Polynomial kernel degree")

  def setDegree(value: Int): this.type = setParameter(degree, value)

  setDefaultParameter(degree, 2)

  val gamma = new Parameter[Double](this, "gamma", "Polynomial or Gaussian kernel scale value (best guess = 1/nFeatures)")

  def setGamma(value: Double): this.type = setParameter(gamma, value)

  setDefaultParameter(gamma, 0.01)

  val coef0 = new Parameter[Double](this, "offset", "Polynomial or tanimoto kernel offset value")

  def setCoef0(value: Double): this.type = setParameter(coef0, value)

  setDefaultParameter(coef0, 0.0)


  def mercerKernel(sparse: Boolean): MercerKernel[_] = {
    sparse match {
      case true => sparseKernel()
      case false => denseKernel()
    }
  }

  def gammaToSigma(gamma:Double): Double = {
    Math.sqrt(1.0/(2.0*gamma))
  }

  def denseKernel(): MercerKernel[Array[Double]] = {
    // adjust parameters so kernels match libSVM
    // note Smile api shows offset in polynomial being subtracted from product, but in code it is added
    getParameter(kernelType).toLowerCase() match {
      case "linear" => new LinearKernel()
      case "poly" => new PolynomialKernel(getParameter(degree), getParameter(gamma), getParameter(coef0))
      case "rbf" => new GaussianKernel(gammaToSigma(getParameter(gamma)))
      case "tanimoto" => new SmileTanimotoKernel(getParameter(degree), getParameter(gamma), getParameter(coef0))
      case v => throw new IllegalArgumentException(s"Unknown kernel type ${v}")
    }
  }

  def sparseKernel(): MercerKernel[SparseArray] = {
    // adjust parameters so kernels match libSVM
    getParameter(kernelType).toLowerCase() match {
      case "linear" => new SparseLinearKernel
      case "poly" => new SparsePolynomialKernel(getParameter(degree), getParameter(gamma), getParameter(coef0))
      case "rbf" => new SparseGaussianKernel(gammaToSigma(getParameter(gamma)))
      case "tanimoto" => new SmileSparseTanimotoKernel(getParameter(degree), getParameter(gamma), getParameter(coef0))
      case v => throw new IllegalArgumentException(s"Unknown kernel type ${v}")
    }
  }
}