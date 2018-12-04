package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DenseVector, SparseVector, VectorBase, Vectors}
import com.cdd.models.utils.HasLogging
import org.apache.log4j.LogManager
import smile.math.SparseArray
import smile.math.kernel.MercerKernel

import scala.collection.JavaConverters._

object SmileUtil extends HasLogging {


  def extractFeaturesAndLabelsToSmileArrays(labels: Vector[Double], features: Vector[VectorBase]):
  (Int, Array[Double], Option[Array[Array[Double]]], Option[Vector[SparseArray]]) = {
    val sparse = features(0) match {
      case v: SparseVector => true
      case _ => false
    }
    val numFeatures = features(0).length()

    if (sparse) {
      val sparseArrays = features.map { v =>
        v match {
          case sv: SparseVector =>
            vectorToSparseArray(sv)
          case _ => throw new IllegalStateException()
        }
      }
      (numFeatures, labels.toArray, None, Some(sparseArrays))
    } else {
      val (y, x) = featuresAndLabelsArrays(labels, features)
      (numFeatures, y, Some(x), None)
    }
  }

  def featuresAndLabelsArrays(labels: Vector[Double], features: Vector[VectorBase]):
  (Array[Double], Array[Array[Double]]) = {
    val y = labels.toArray
    val x = new Array[Array[Double]](y.length)
    features(0) match {
      case v: SparseVector => logger.warn("Creating dense arrays from sparse vectors!")
      case _ =>
    }
    features.zipWithIndex.foreach { case (f, i) =>
      x(i) = f.toArray()
    }
    (y, x)
  }

  def vectorToSparseArray(v: VectorBase): SparseArray = {
    val sa = new SparseArray()
    v match {
      case sv: SparseVector =>
        sv.indices().zip(sv.values()).map { case (i, d) => sa.append(i, d) }
      case dv: DenseVector =>
        dv.toArray().zipWithIndex.foreach { case (d, i) => sa.append(i, d) }
    }
    sa
  }

  def tanimotoOld(a: SparseArray, b: SparseArray): Double = {

    val aSqr = a.iterator().asScala.foldLeft(0.0)((sum, entry) => sum + entry.x * entry.x)
    val bSqr = b.iterator().asScala.foldLeft(0.0)((sum, entry) => sum + entry.x * entry.x)
    val aCrossB = a.iterator().asScala.foldLeft(0.0) { (sum, entry) =>
      val bx = b.get(entry.i)
      sum + bx * entry.x
    }
    val t = aCrossB / (aSqr + bSqr - aCrossB)
    assert(t >= 0 && t <= 1.0)
    t
  }

  def tanimoto(a: SparseArray, b: SparseArray): Double = {

    // optimized version of Tanimoto
    val it1 = a.iterator()
    val it2 = b.iterator()

    var aCrossB = .0
    var aSqr = .0
    var bSqr = .0

    var e1 = if (it1.hasNext) it1.next else null
    var e2 = if (it2.hasNext) it2.next else null
    if (e1 != null)
      aSqr += e1.x * e1.x
    if (e2 != null)
      bSqr += e2.x * e2.x


    while (e1 != null || e2 != null) {
      var incE1 = false
      var incE2 = false
      if (e1 != null && e2 != null) {
        if (e1.i == e2.i) {
          aCrossB += e1.x * e2.x
          incE1 = true
          incE2 = true
        }
        else if (e1.i > e2.i) {
          incE2 = true
        }
        else {
          incE1 = true
        }
      }
      else if (e1 != null) {
        incE1 = true
      }
      else if (e2 != null) {
        incE2 = true
      }

      if (incE1) {
        e1 = if (it1.hasNext) it1.next else null
        if (e1 != null)
          aSqr += e1.x * e1.x
      }
      if (incE2) {
        e2 = if (it2.hasNext) it2.next else null
        if (e2 != null)
          bSqr += e2.x * e2.x
      }
    }

    val t = aCrossB / (aSqr + bSqr - aCrossB)
    assert(t >= 0 && t <= 1.0)
    //assert(t == tanimotoOld(a, b))
    t
  }

  def resetSmileMathRandom(): Unit = {
    /*
    Resets the smile random number generator - useful to ensure reproducibility

    Note the Smile random number generator in smile.math.Math is a ThreadLocal
    This reset only works for single threaded algorithms running in the current thread.
    Works for SVMs, but will probably not work for random forest
     */
    val clazz = Class.forName("smile.math.Math")
    val firstRngField = clazz.getDeclaredField("firstRNG")
    firstRngField.setAccessible(true)
    val firstRng = firstRngField.get(null).asInstanceOf[Boolean]
    if (!firstRng) {
      firstRngField.set(null, true)
      val randomField = clazz.getDeclaredField("random")
      randomField.setAccessible(true)
      val random = randomField.get(null).asInstanceOf[ThreadLocal[smile.math.Random]]
      random.remove()
    }
  }
}

@SerialVersionUID(1000L)
class SmileTanimotoKernel(val degree: Int, val gamma: Double, val coef0: Double) extends MercerKernel[Array[Double]]
  with Serializable{
  override def k(t1: Array[Double], t2: Array[Double]): Double = {
    val v1 = Vectors.Dense(t1)
    val v2 = Vectors.Dense(t2)
    val tanimoto = v1.tanimoto(v2)
    Math.pow(gamma * tanimoto + coef0, degree.toDouble)
  }
}

@SerialVersionUID(1000L)
class SmileSparseTanimotoKernel(val degree: Int, val gamma: Double, val coef0: Double) extends MercerKernel[SparseArray]
  with Serializable {
  override def k(x: SparseArray, y: SparseArray): Double = {
    val tanimoto = SmileUtil.tanimoto(x, y)
    Math.pow(gamma * tanimoto + coef0, degree.toDouble)
  }
}

