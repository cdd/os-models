package com.cdd.models.utils

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics


/**
  * Summary statistics which supports sparse data though batch update of zeros
  */
@SerialVersionUID(1000L)
class SummaryStatistics() extends Serializable {

  private var sum = 0.0
  private var sqrSum = 0.0
  private var count = 0L
  private var maxValue = -Double.MaxValue
  private var minValue = Double.MaxValue

  def addValue(v:Double): Unit = {
    sum += v
    sqrSum += (v*v)
    count += 1L
    if (v < minValue)
      minValue = v
    if (v > maxValue)
      maxValue = v
  }

  def addZeros(noZeros:Long): Unit = {
    count += noZeros
     if (0.0 < minValue)
      minValue = 0.0
    if (0.0 > maxValue)
      maxValue = 0.0
  }

  def no:Long = count

  def mean():Double = {
    if (count > 0)
      return sum/count.toDouble
    Double.NaN
  }

  def variance(correctBias:Boolean=false):Double = {
    // variance without bias correction
    if (count == 1L)
      return 0.0
    val n = if (correctBias) count-1 else count
    if (n > 0) {
      val m = mean()
      return sqrSum/n.toDouble - m*m
    }
    Double.NaN
  }

  def max() :Double =  maxValue

  def min(): Double = minValue

  def standardDeviation(correctBias:Boolean=false): Double = {
    val variance = this.variance(correctBias)
    if (variance.isNaN) {
      return Double.NaN
    }
    math.sqrt(variance)
  }
}
