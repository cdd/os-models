package com.cdd.models.datatable

import breeze.linalg.{StorageVector, DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.DoubleLiteralContext

object Vectors {

  def Dense(values:Array[Double]): DenseVector = new DenseVector(values)

  def Sparse(indices: Array[Int], values: Array[Double], length: Int): SparseVector = new SparseVector(indices, values, length)

}

trait VectorBase extends Serializable{
  type Self <: VectorBase
  def length(): Int
  def apply(index:Int): Double
  def toArray(): Array[Double]
  def tanimoto(other: VectorBase ): Double
  def forEachActive(block:(Int, Double) => Unit): Unit
  def copy(): Self
}

@SerialVersionUID(7079606408492254930L)
class DenseVector(val rawVector: BreezeDenseVector[Double]) extends VectorBase with Serializable {
  type Self = DenseVector

  def this(values:Array[Double]) = this(BreezeDenseVector(values))

  def this(values:Vector[Double]) = this(BreezeDenseVector(values.toArray))

  override def apply(index: Int):Double = rawVector(index)

  override def length(): Int = rawVector.length

  override def toString(): String = rawVector.toArray.mkString("|")

  override def toArray(): Array[Double] = rawVector.toArray

  override def tanimoto(other: VectorBase ): Double = {
    assert(length() == other.length())
    var aSqr = .0
    var bSqr = .0
    var aCrossB =   .0
    toArray().zip(other.toArray()).foreach { case(a, b) =>
      aSqr += a*a
      bSqr += b*b
      aCrossB += a*b
    }
    
    aCrossB/(aSqr+bSqr-aCrossB)
  }

  override def copy(): DenseVector = {
   new DenseVector(toArray().clone())
  }

  override def forEachActive(block:(Int, Double) => Unit) : Unit = {
    toArray().zipWithIndex.foreach { case (v, i) => block(i, v)}
  }

  def values(): Array[Double] = rawVector.data

  def argmax(): Int = {
    values().zipWithIndex.foldLeft(-1, -Double.NaN) { case((argmax, max), (value, index)) =>
        if (value > max)
          (index, value)
        else
          (argmax, max)
    }._1
  }
}

@SerialVersionUID(-6372288804361637558L)
class SparseVector(val rawVector: BreezeSparseVector[Double]) extends VectorBase with Serializable {
  type Self = SparseVector

   def this(indices: Array[Int], values: Array[Double], length: Int) = this(new BreezeSparseVector[Double](indices, values, length))

  override def apply(index: Int):Double = rawVector(index)

  override def length(): Int = rawVector.length

  override def toString: String = rawVector.length.toString + ":" + rawVector.index.mkString("|") + ":" + rawVector.data.mkString("|")

  override def toArray(): Array[Double] = rawVector.toArray

  def indices(): Array[Int] = rawVector.index

  def values(): Array[Double] = rawVector.data

  // private SparseVector method adapted from org/apache/spark/ml/linalg/Vectors.scala
  def slice( selectedIndices: Vector[Int]): SparseVector = {
    var currentIdx = 0
    val (sliceInds, sliceVals) = selectedIndices.flatMap { origIdx =>
      val iIdx = java.util.Arrays.binarySearch(indices(), origIdx)
      val i_v = if (iIdx >= 0) {
        Iterator((currentIdx, values()(iIdx)))
      } else {
        Iterator()
      }
      currentIdx += 1
      i_v
    }.unzip
    Vectors.Sparse(sliceInds.toArray, sliceVals.toArray, selectedIndices.length)
  }

  override def tanimoto(o: VectorBase ): Double = {
    val other = o.asInstanceOf[SparseVector]
    assert(length() == other.length())
    val aSqr = values().map { v => v*v }.sum
    val bSqr = other.values().map { v => v*v }.sum
    //val aCrossB = indices().intersect(other.indices()).map { i => apply(i)*other(i) }.sum
    val aCrossB = indices().map { i => apply(i)*other(i) }.sum
    aCrossB/(aSqr+bSqr-aCrossB)
  }

  override def copy(): SparseVector = {
    new SparseVector(indices().clone(), values().clone(), length())
  }

  override def forEachActive(block:(Int, Double) => Unit) : Unit = {
    indices().zip(values()).foreach { case (i, v) => block(i, v)}
  }
}