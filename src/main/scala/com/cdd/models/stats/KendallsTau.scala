package com.cdd.models.stats

import com.cdd.models.utils.Modifier


case class Observation(xValue: Double, xModifier: Modifier, yValue: Double, yModifier: Modifier) {

  private[stats] def compareObservation(other: Observation): (Int, Int) = {
    val deltaX = Observation.compare(xValue, xModifier, other.xValue, other.xModifier)
    val deltaY = Observation.compare(yValue, yModifier, other.yValue, other.yModifier)

    (deltaX, deltaY)
  }
}

object Observation {
  def compare(x1: Double, x1m: Modifier, x2: Double, x2m: Modifier): Int = {
    x1m match {
      case Modifier.EQUAL => compareFirstEqual(x1, x2, x2m)
      case Modifier.LESS_THAN => compareFirstLessThan(x1, x2, x2m)
      case Modifier.GREATER_THAN => compareFirstLessThan(x1, x2, x2m)
    }
  }

  private def compareFirstEqual(x1: Double, x2: Double, x2m: Modifier): Int = {
    x2m match {
      case Modifier.EQUAL =>
        if (x1 == x2)
          0
        else if (x1 > x2)
          1
        else
          -1
      case Modifier.LESS_THAN => if (x1 > x2) 1 else 0
      case Modifier.GREATER_THAN => if (x1 < x2) -1 else 0
      case _ => throw new IllegalArgumentException
    }
  }

  private def compareFirstLessThan(x1: Double, x2: Double, x2m: Modifier): Int = {
    x2m match {
      case Modifier.EQUAL => if (x2 >= x1) -1 else 0
      case Modifier.LESS_THAN => 0
      case Modifier.GREATER_THAN => throw new IllegalStateException("Unable to simultaneously process left and right censored data")
      case _ => throw new IllegalArgumentException
    }
  }

  private def compareFirstGreaterThan(x1: Double, x2: Double, x2m: Modifier): Int = {
    x2m match {
      case Modifier.EQUAL => if (x2 <= x1) -1 else 0
      case Modifier.LESS_THAN => throw new IllegalStateException("Unable to simultaneously process left and right censored data")
      case Modifier.GREATER_THAN => 0
      case _ => throw new IllegalArgumentException
    }
  }

}

object KendallsTau {

  def computeTauB(xs: Vector[Double], ys: Vector[Double], xCensors: Option[Vector[Modifier]],
                  yCensors: Option[Vector[Modifier]]=None):Double = {

    val observations = xs.zip(ys).zipWithIndex.map { case ((x, y), i) =>
      val xC = xCensors match {
        case Some(xC) => xC(i)
        case None => Modifier.EQUAL
      }
      val yC = yCensors match {
        case Some(yC) => yC(i)
        case None => Modifier.EQUAL
      }
      Observation(x, xC, y, yC)
    }

    var nTiesX = 0L
    var nTiesY = 0L
    var sumOfSigns = 0L // sum of no concordant pairs - no of discordant pairs

    val size = observations.size
    for (i <- 0 until size) {
      for (j <- i + 1 until size) {
        val (deltaX, deltaY) = observations(i).compareObservation(observations(j))
        if (deltaX == 0) nTiesX += 1L
        if (deltaY == 0) nTiesY += 1L
        sumOfSigns += (deltaX * deltaY).toLong
      }
    }

    val nPairs = (size.toLong*(size.toLong-1L))/2L
    val nX = nPairs - nTiesX
    val nY = nPairs - nTiesY

    sumOfSigns.toDouble/Math.sqrt(nX.toDouble*nY.toDouble)

  }
}
