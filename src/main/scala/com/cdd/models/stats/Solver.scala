package com.cdd.models.stats

import org.apache.commons.math3.analysis.UnivariateFunction
import org.apache.commons.math3.analysis.solvers.BrentSolver

object Solver {

  implicit def funToUnivariate(fn: Double => Double) :UnivariateFunction = new UnivariateFunction {
    override def value(v: Double): Double = {
      fn(v)
    }
  }

  def findXforY(fn: Double => Double, y: Double, min: Double, max: Double, maxEval: Int = 100, accuracy: Double=1.0e-6): Double = {
    findRoot(v => fn(v) - y, min, max, maxEval, accuracy)
  }

  def findRoot(fn: Double => Double, min: Double, max: Double, maxEval: Int = 100, accuracy: Double=1e-6): Double = {
    val solver = new BrentSolver(accuracy)
    solver.solve(maxEval, fn, min, max)
  }

}
