package com.cdd.models.universalmetric

import scala.collection.mutable

class DiscreteDataHistogram(val values: Vector[Option[Double]]) {

  private val flatValues = values.flatten
  private val histogram: Map[Double, Int] = buildHistogram()
  private val mostPrevalent: Vector[(Double, Int)] = histogram.toSeq.sortBy(-_._2).toVector.slice(0, 10)

  private def buildHistogram() = {
    flatValues.foldLeft(mutable.Map[Double, Int]()) { case (map, v) =>
      val count = map.getOrElse(v, 0) + 1
      map(v) = count
      map
    }.toMap
  }


  def binaryClassification(): Option[Vector[Option[Double]]] = {
    if (!isClassification())
      return None
    val binaryValues = values.map {
      case Some(v) => Some(valueToBinaryClass(v, mostCommonHigher(), mid()))
      case None => None
    }
    Some(binaryValues)
  }

  def mostCommonHigher(): Boolean = {
    val v1 = mostPrevalent(0)._1
    val v2 = mostPrevalent(1)._1
    v1 > v2
  }

  private def mid() = {

    val v1 = mostPrevalent(0)._1
    val v2 = mostPrevalent(1)._1

    (v1 + v2) / 2.0
  }


  private def valueToBinaryClass(v: Double, mostCommonHigher:Boolean, mid:Double) = {
    val v1 = mostPrevalent(0)._1
    val v2 = mostPrevalent(1)._1

    val highValue = v > mid
    var b = 0.0
    if (highValue && !mostCommonHigher) b = 1.0
    if (!highValue && mostCommonHigher) b = 1.0
    b
  }

   def isClassification(): Boolean = {
    if (histogram.size == 1) return false
    if (histogram.size == 2) return true
    if (histogram.size * 10 > flatValues.size) return false

    val top2count = mostPrevalent(0)._2 + mostPrevalent(1)._2
    if (top2count * 1.25 > flatValues.size)
      return true

    false
  }
}
