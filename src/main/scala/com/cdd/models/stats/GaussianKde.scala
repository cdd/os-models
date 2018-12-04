package com.cdd.models.stats

import com.cdd.models.utils.Util
import org.apache.commons.math3.analysis.function.Gaussian
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics


trait Kde {
  val values: Vector[Double]

  def probability(x: Double): Double

  def firstDerivative(x: Double): Double

  def secondDerivative(x: Double): Double

  def cdf(x: Double): Double

  def proportion(x:Double) : Double = {
    val p = cdf(x)
    if (p > 0.5) 1.0-p else p
  }


  private def grid(nPoints: Int, fn: Double => Double, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double)] = {
    xGrid(nPoints, startOption, endOption).map { x =>
      val y = fn(x)
      (x, y)
    }
  }

  def probabilityGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double)] = {
    grid(nPoints, probability, startOption, endOption)
  }

  def firstDerivativeGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double)] = {
    grid(nPoints, firstDerivative, startOption, endOption)
  }

  def secondDerivativeGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double)] = {
    grid(nPoints, secondDerivative, startOption, endOption)
  }

  def cdfGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double)] = {
    grid(nPoints, cdf, startOption, endOption)
  }

  def fullGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[(Double, Double, Double, Double, Double)] = {
    val density = probabilityGrid(nPoints, startOption, endOption)
    val first = firstDerivativeGrid(nPoints, startOption, endOption)
    val second = secondDerivativeGrid(nPoints, startOption, endOption)
    val cdf = cdfGrid(nPoints, startOption, endOption)

    density.zip(first).zip(second).zip(cdf).map { case ((((x1, d), (x2, f)), (x3, s)), (x4, c)) =>
      assert(x1 == x2)
      assert(x2 == x3)
      assert(x3 == x4)
      (x1, d, f, s, c)
    }

  }

  protected def xGrid(nPoints: Int, startOption: Option[Double], endOption: Option[Double]): Vector[Double] = {
    val start = startOption.getOrElse(values.min)
    val end = endOption.getOrElse(values.max)

    val spacing = (end - start) / (nPoints.toDouble - 1.0)
    (0 until nPoints).map { p =>
      start + p.toDouble * spacing
    }.toVector
  }
}

class GaussianKde(valuesIn: Vector[Double], h: Double = -1.0) extends Kde {

  val (values: Vector[Double], bandwidth: Double, mean: Double, standardDeviation: Double,
  variance: Double, gaussian: GaussianDerivatives) = init()

  private def init() = {
    val values = valuesIn.sorted
    val stats = new DescriptiveStatistics(values.toArray)
    val standardDeviation = stats.getStandardDeviation
    val mean = stats.getMean
    val variance = stats.getVariance
    val bandwidth =
      if (h != -1.0) {
        h
      } else {
        1.06 * standardDeviation * Math.pow(values.length.toDouble, -0.2)
      }
    require(bandwidth > 0)
    val gaussian = new GaussianDerivatives(bandwidth)
    (values, bandwidth, mean, standardDeviation, variance, gaussian)
  }

  private def valueAtX(x: Double, fn: Double => Double): Double = {
    // TODO can ignore distant values
    val sum = values.map { value =>
      fn(x - value)
    }.sum
    sum / values.length.toDouble
  }

  def probability(x: Double): Double = {
    valueAtX(x, gaussian.value)
  }

  def firstDerivative(x: Double): Double = {
    valueAtX(x, gaussian.firstDerivative)
  }

  def secondDerivative(x: Double): Double = {
    valueAtX(x, gaussian.secondDerivative)
  }

  def cdf(x: Double): Double = {
    valueAtX(x, gaussian.integralBelow)
  }

}

class AdaptiveSamplePointKde(valuesIn: Vector[Double]) extends Kde {

  val (values: Vector[Double], mean: Double, standardDeviation: Double,
  variance: Double, bandwidths: Vector[Double]) = init()


  def init() = {
    val values = valuesIn.sorted

    val stats = new DescriptiveStatistics(values.toArray)
    val standardDeviation = stats.getStandardDeviation
    val mean = stats.getMean
    val variance = stats.getVariance

    val gKde = new GaussianKde(values)
    val n = values.length.toDouble
    val bandwidth = gKde.bandwidth
    val densityGuess = values.map { v => gKde.probability(v) }
    val (densityPeak, densityPeakIndex) = densityGuess.zipWithIndex.maxBy(_._1)

    val adaptiveBandwidths = densityGuess.map { g =>
      bandwidth / (g * n)
      // scaling to the inverse seems to smear out the tails too much, so scaled to inverse root
      // val g2 = Math.max(g, densityPeak/3.0)
      // bandwidth / (g2 * n)
      //bandwidth / Math.sqrt(g * n)
    }

    val geoMean = geometricMean(adaptiveBandwidths)
    val bmean = adaptiveBandwidths.sum / n
    val scaledAdaptiveBandwidths = adaptiveBandwidths.map { b => b * bandwidth / geoMean }

    // val adaptivePeakDensity = adaptiveBandwidths(densityPeakIndex)
    // val scaledAdaptiveBandwidths = adaptiveBandwidths.map { b => b * bandwidth / (2.0*adaptivePeakDensity) }

    (values, mean, standardDeviation, variance, scaledAdaptiveBandwidths)
  }

  private def geometricMean(x: Vector[Double]): Double = {
    val logSum = x.map {
      Math.log
    }.sum
    Math.exp(logSum / x.length.toDouble)
  }

  private def valueAtX(x: Double, fn: (GaussianDerivatives, Double) => Double): Double = {
    // TODO can ignore distant values
    val sum = values.zip(bandwidths).map { case (value, bandwidth) =>
      val g = new GaussianDerivatives(bandwidth)
      fn(g, x - value)
    }.sum
    sum / values.length.toDouble
  }


  def probability(x: Double): Double = {
    val fn = (g: GaussianDerivatives, x: Double) => g.value(x)
    valueAtX(x, fn)
  }

  def firstDerivative(x: Double): Double = {
    val fn = (g: GaussianDerivatives, x: Double) => g.firstDerivative(x)
    valueAtX(x, fn)
  }

  def secondDerivative(x: Double): Double = {
    val fn = (g: GaussianDerivatives, x: Double) => g.secondDerivative(x)
    valueAtX(x, fn)
  }

  def cdf(x: Double): Double = {
    val fn = (g: GaussianDerivatives, x: Double) => g.integralBelow(x)
    valueAtX(x, fn)
  }

}

class GaussianDerivatives(val sigma: Double) {
  val gaussian = new Gaussian(0, sigma)
  private val sigma2 = sigma * sigma
  private val sigma4 = sigma2 * sigma2

  def value(x: Double): Double = gaussian.value(x)

  // see http://campar.in.tum.de/Chair/HaukeHeibelGaussianDerivatives
  def firstDerivative(x: Double): Double = {
    val g = gaussian.value(x)
    val h = x / sigma2
    -h * g
  }

  def secondDerivative(x: Double): Double = {
    val g = gaussian.value(x)
    val h = (sigma2 - x * x) / sigma4
    -h * g
  }

  def integralBelow(x: Double): Double = {
    Util.normalCdf(x, sigma)
  }
}
