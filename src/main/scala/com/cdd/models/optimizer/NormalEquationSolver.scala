package com.cdd.models.optimizer

// ported from org.apache.spark.ml.optim.NormalEquationSolver.scala

import scala.collection.mutable
import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import com.cdd.models.datatable.DenseVector


/**
 * A class to hold the solution to the normal equations A^T^ W A x = A^T^ W b.
 *
 * @param coefficients The least squares coefficients. The last element in the coefficients
 *                     is the intercept when bias is added to A.
 * @param aaInv An option containing the upper triangular part of (A^T^ W A)^-1^, in column major
 *              format. None when an optimization program is used to solve the normal equations.
 * @param objectiveHistory Option containing the objective history when an optimization program is
 *                         used to solve the normal equations. None when an analytic solver is used.
 */

private[optimizer] class NormalEquationSolution(
    val coefficients: Array[Double],
    val aaInv: Option[Array[Double]],
    val objectiveHistory: Option[Array[Double]])

/**
 * Interface for classes that solve the normal equations locally.
 */
private[optimizer] sealed trait NormalEquationSolver {

  /** Solve the normal equations from summary statistics. */
  def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution
}

/**
 * A class that solves the normal equations directly, using Cholesky decomposition.
 */
private[optimizer] class CholeskySolver extends NormalEquationSolver {

  override def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution = {
    val k = abBar.length()
    val x = CholeskyDecomposition.solve(aaBar.values(), abBar.values())
    val aaInv = CholeskyDecomposition.inverse(aaBar.values(), k)

    new NormalEquationSolution(x, Some(aaInv), None)
  }
}

/**
 * A class for solving the normal equations using Quasi-Newton optimization methods.
 */
private[optimizer] class QuasiNewtonSolver(
    fitIntercept: Boolean,
    maxIter: Int,
    tol: Double,
    l1RegFunc: Option[(Int) => Double]) extends NormalEquationSolver {

  override def solve(
      bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector): NormalEquationSolution = {
    val numFeatures = aBar.length()
    val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures
    val initialCoefficientsWithIntercept = new Array[Double](numFeaturesPlusIntercept)
    if (fitIntercept) {
      initialCoefficientsWithIntercept(numFeaturesPlusIntercept - 1) = bBar
    }

    val costFun =
      new NormalEquationCostFun(bBar, bbBar, abBar, aaBar, aBar, fitIntercept, numFeatures)
    val optimizer = l1RegFunc.map { func =>
      new BreezeOWLQN[Int, BDV[Double]](maxIter, 10, func, tol)
    }.getOrElse(new BreezeLBFGS[BDV[Double]](maxIter, 10, tol))

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      new BDV[Double](initialCoefficientsWithIntercept))

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    val x = state.x.toArray.clone()
    new NormalEquationSolution(x, None, Some(arrayBuilder.result()))
  }

  /**
   * NormalEquationCostFun implements Breeze's DiffFunction[T] for the normal equation.
   * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
   * It's used in Breeze's convex optimization routines.
   */
  private class NormalEquationCostFun(
      bBar: Double,
      bbBar: Double,
      ab: DenseVector,
      aa: DenseVector,
      aBar: DenseVector,
      fitIntercept: Boolean,
      numFeatures: Int) extends DiffFunction[BDV[Double]] {

    private val numFeaturesPlusIntercept = if (fitIntercept) numFeatures + 1 else numFeatures

    override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
      val coef = new DenseVector(coefficients.data)
      if (fitIntercept) {
        var j = 0
        var dotProd = 0.0
        val coefValues = coef.values()
        val aBarValues = aBar.values()
        while (j < numFeatures) {
          dotProd += coefValues(j) * aBarValues(j)
          j += 1
        }
        coefValues(numFeatures) = bBar - dotProd
      }
      val aax = new DenseVector(new Array[Double](numFeaturesPlusIntercept))
      Blas.dspmv(numFeaturesPlusIntercept, 1.0, aa, coef, 1.0, aax)
      // loss = 1/2 (b^T W b - 2 x^T A^T W b + x^T A^T W A x)
      val loss = 0.5 * bbBar - Blas.dot(ab, coef) + 0.5 * Blas.dot(coef, aax)
      // gradient = A^T W A x - A^T W b
      Blas.axpy(-1.0, ab, aax)
      (loss, BDV(aax.values()))
    }
  }
}

/**
 * Exception thrown when solving a linear system Ax = b for which the matrix A is non-invertible
 * (singular).
 */
private[optimizer] class SingularMatrixException(message: String, cause: Throwable)
  extends IllegalArgumentException(message, cause) {

  def this(message: String) = this(message, null)
}
