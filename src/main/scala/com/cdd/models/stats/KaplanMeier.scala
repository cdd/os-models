package com.cdd.models.stats

import com.cdd.models.utils.{CensoredValue, Modifier}

import scala.collection.mutable.ArrayBuffer


object KaplanMeier {

  def censoredType(items: Vector[CensoredValue]): Modifier = {
    val hasLeft = items.exists(_.getModifier == Modifier.LESS_THAN)
    val hasRight = items.exists(_.getModifier == Modifier.GREATER_THAN)
    if (hasLeft) {
      require(!hasRight)
      return Modifier.LESS_THAN
    }
    if (hasRight) {
      require(!hasLeft)
      return Modifier.GREATER_THAN
    }
    Modifier.EQUAL
  }

  def orderLeft(a: CensoredValue, b: CensoredValue): Boolean = {
    if (a.getValue > b.getValue)
      true
    else if (a.getValue == b.getValue && a.getModifier == Modifier.LESS_THAN && b.getModifier == Modifier.EQUAL)
      true
    else
      false
  }

  def orderRight(a: CensoredValue, b: CensoredValue): Boolean = {
    if (a.getValue < b.getValue)
      true
    else if (a.getValue == b.getValue && a.getModifier == Modifier.GREATER_THAN && b.getModifier == Modifier.EQUAL)
      true
    else
      false
  }

  def orderItems(items: Vector[CensoredValue], leftCentered: Boolean = true): Vector[CensoredValue] = {
    if (leftCentered)
      items.sortWith(orderLeft)
    else
      items.sortWith(orderRight)
  }
}

case class KaplanMeierTableEntry(index: Int, value: Double, total: Int, noUncensored: Int, f: Double) {
  def ratio: Double = (total - noUncensored).toDouble / total.toDouble
}

class KaplanMeier(val items: Vector[CensoredValue]) {

  val censoredType: Modifier = KaplanMeier.censoredType(items)
  require(censoredType != Modifier.EQUAL)
  val orderedItems:Vector[CensoredValue] = KaplanMeier.orderItems(items, censoredType == Modifier.LESS_THAN)
  val table:Vector[KaplanMeierTableEntry] = buildTable()
  val (mean, variance) = calculateMeanAndVariance()

  private def buildTable() = {
    var index = 0
    var noUncensored = 0
    val size = orderedItems.size
    var start = 0
    val table = ArrayBuffer[KaplanMeierTableEntry]()
    var last:KaplanMeierTableEntry = null

    orderedItems.zipWithIndex.foreach { case (item, position) =>
      if (item.getModifier == Modifier.EQUAL)
        noUncensored += 1
      if (position + 1 < size && orderedItems(position + 1).getValue == item.getValue) {
        start += 1
      }
      else if (item.getModifier == Modifier.EQUAL) {
        val total = size - position + start
        val f = if (index == 0) 1.0 else last.f * last.ratio
        val entry = KaplanMeierTableEntry(index, item.getValue, total, noUncensored, f)
        table.append(entry)
        index += 1
        start = 0
        noUncensored = 0
        last = entry
      }
    }
    table.toVector
  }

  private def calculateMeanAndVariance() = {
    var last:KaplanMeierTableEntry = null
    var sum = 0.0
    var sqrSum = 0.0

    table.reverseIterator.foreach { entry =>
      val lastF = if (last == null) .0 else last.f
      last = entry
      val step = entry.f - lastF
      sum += entry.value * step
      sqrSum += entry.value*entry.value*step
    }
    val mean = sum
    val n = items.size.toDouble
    // Biased variance
    val variance = (sqrSum - mean*mean)*n/(n-1)

    (mean, variance)
  }

}
