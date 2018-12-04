package com.cdd.models.datatable

import com.cdd.models.datatable.DataTableColumn.NoneValuesPresentException

import scala.util.Try

object DataTableColumn {

  class NoneValuesPresentException extends Exception("Empty values present!")

  class AllItemsNoneException extends Exception("All items in column are None!")

  private def determineListClass(values: Seq[Option[_]]): Class[_] = {
    def item = values.find(_.isDefined)

    if (item.isEmpty) {
      throw new AllItemsNoneException
    }
    item.get.get.getClass
  }

  def fromVector[A](title: String, values: Vector[A]): DataTableColumn[A] = {
    val vals = values.map { v =>
      require(!v.isInstanceOf[Option[_]])
      require(v != null)
      Some(v)
    }
    new DataTableColumn[A](title, vals)
  }


  def stringToIntColumn(column: DataTableColumn[String]): Option[DataTableColumn[Int]] = {
    require(column.clazz == classOf[String])

    val allInts = column.values.forall {
      case Some(v: String) => Try {
        v.toInt
      }.isSuccess
      case None => true
    }
    if (!allInts) return None
    val ints = column.values.map {
      case Some(s: String) => Some(s.toInt)
      case None => None
    }

    Some(new DataTableColumn(column.title, ints))
  }

  def stringToDoubleColumn(column: DataTableColumn[String]): Option[DataTableColumn[Double]] = {
    require(column.clazz == classOf[String])

    val allDoubles = column.values.forall {
      case Some(v: String) => Try {
        v.toDouble
      }.isSuccess
      case None => true
    }
    if (!allDoubles) return None
    val doubles = column.values.map {
      case Some(s: String) => Some(s.toDouble)
      case None => None
    }

    Some(new DataTableColumn(column.title, doubles))
  }
}

import DataTableColumn.determineListClass

/**
  * Created by gjones on 7/23/17.
  */
@SerialVersionUID(1000L)
class DataTableColumn[A](val title: String, val values: Vector[Option[A]], val clazz: Class[A]) extends Serializable {

  def this(title: String, values: Vector[Option[A]]) {
    this(title, values, determineListClass(values).asInstanceOf[Class[A]])
  }

  def extractIndices(indices: Seq[Int]): DataTableColumn[A] = {
    new DataTableColumn[A](title, indices.map(values(_)).toVector, clazz)
  }

  def removeIndices(indices: Set[Int]): DataTableColumn[A] = {
    val filteredValues = values.zipWithIndex.filterNot { case (v, i) => indices.contains(i) }.map { case (v, i) => v }
    new DataTableColumn[A](title, filteredValues, clazz)
  }

  def matchingIndices(fn: (A) => Boolean, notMatching:Boolean=false): Set[Int] = {
    values.zipWithIndex.filter { case (v, _) =>
      val test = v match {
        case Some(v2) => fn(v2)
        case None => false
      }
      if (notMatching) !test else test
    }
      .map { case (_, i) => i }.toSet
  }

  def valueToString(index: Int): String = {
    values(index) match {
      case None => ""
      case Some(v) =>
        v match {
          case sv: String => sv
          case iv: Int => iv.toString
          case dv: Double => dv.toString
          case sv: SparseVector => sv.toString()
          case dv: DenseVector => dv.toString()
          case v: Vector[Any] => v.toArray.mkString("|")
          case _ => throw new IllegalStateException("Unsupported column type")
        }
    }
  }

  def matchingIndices(matches: Vector[A]): Vector[Int] = {
    values.zipWithIndex.filter { case (v, _) =>
      v match {
        case Some(v) => matches.contains(v)
        case None => false
      }
    }.map {
      _._2
    }
  }

  private[datatable] def stringHeader(): String = {
    clazz match {
      case c if c == classOf[String] => title + " [String]"
      case c if c == classOf[Int] || c == classOf[java.lang.Integer] => title + " [Int]"
      case c if c == classOf[Double] || c == classOf[java.lang.Double] => title + " [Double]"
      case c if c == classOf[SparseVector] => title + " [SparseVector]"
      case c if c == classOf[DenseVector] => title + " [DenseVector]"
      case c if c == classOf[Vector[Any]] => title + " [String]" // to allow writing of optimizaton results, but will be parsed as a string on reading.
      case _ => throw new IllegalStateException("Unsupported column type")
    }
  }

  def slice(start: Int = 0, end: Int = length): DataTableColumn[A] = {
    require(start >= 0 && start < length)
    require(start < end)
    require(end > 0 && end <= length)
    val slice = values.zipWithIndex.filter { case (value, index) => index >= start && index < end }.map {
      _._1
    }
    new DataTableColumn[A](title, slice, clazz)
  }

  def isDouble(): Boolean = clazz == classOf[Double] || clazz == classOf[java.lang.Double]

  def isInt(): Boolean = clazz == classOf[Int] || clazz == classOf[java.lang.Integer]

  def isVector(): Boolean = clazz == classOf[SparseVector] || clazz == classOf[DenseVector]

  def isString(): Boolean = clazz == classOf[String] || clazz == classOf[java.lang.String]

  def toDoubles(): Vector[Double] = {
    require(isDouble())
    checkNoneValues()
    val doubleValues = values.flatten.asInstanceOf[Vector[Double]]
    assert(doubleValues.length == length())
    doubleValues
  }

  def toStrings(): Vector[String] = {
    require(isString())
    checkNoneValues()
    val stringValues = values.flatten.asInstanceOf[Vector[String]]
    assert(stringValues.length == length())
    stringValues
  }

  def toInts(): Vector[Int] = {
    require(isInt())
    checkNoneValues()
    val intValues = values.flatten.asInstanceOf[Vector[Int]]
    assert(intValues.length == length())
    intValues
  }

  def toVector(): Vector[VectorBase] = {
    require(isVector())
    checkNoneValues()
    val vectorValues = values.flatten.asInstanceOf[Vector[VectorBase]]
    assert(vectorValues.length == length())
    vectorValues
  }

  def length(): Int = values.size

  def hasNoneValues(): Boolean = {
    values.exists(_.isEmpty)
  }

  def checkNoneValues(): Unit = {
    if (hasNoneValues())
      throw new NoneValuesPresentException
  }

  private[datatable] def nullValuesIndices(): Set[Int] = {
    values.zipWithIndex.filter {
      _._1 == None
    }.map {
      _._2
    }.toSet
  }

  def append(other: DataTableColumn[A]): DataTableColumn[A] = {
    require(other.clazz == clazz)
    require(other.title == title)

    new DataTableColumn[A](title, values ++ other.values, clazz)
  }

  def uniqueValues(): Vector[A] = {
    values.flatten.distinct
  }

}
