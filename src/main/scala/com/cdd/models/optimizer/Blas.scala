package com.cdd.models.optimizer


import com.cdd.models.datatable.{DenseVector, SparseVector, VectorBase}
import com.github.fommil.netlib.{BLAS, F2jBLAS}

//  code based on routines in org.apache.spark.ml.linalg.BLAS.scala

object Blas {

  private val f2jBlas: BLAS = new F2jBLAS
  private val nativeBlas: BLAS = BLAS.getInstance()

  /**
    * dot(x, y)
    */
  def dot(x: VectorBase, y: VectorBase): Double = {
    require(x.length() == y.length(),
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
        " x.size = " + x.length() + ", y.size = " + y.length())
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
    * dot(x, y)
    */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.length()
    f2jBlas.ddot(n, x.values(), 1, y.values(), 1)
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values()
    val xIndices = x.indices()
    val yValues = y.values()
    val nnz = xIndices.length

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values()
    val xIndices = x.indices()
    val yValues = y.values()
    val yIndices = y.indices()
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  /**
    * y += a * x
    */
  def axpy(a: Double, x: VectorBase, y: VectorBase): Unit = {
    require(x.length == y.length)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            axpy(a, sx, dy)
          case dx: DenseVector =>
            axpy(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"axpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"axpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  /**
    * y += a * x
    */
  private def axpy(a: Double, x: DenseVector, y: DenseVector): Unit = {
    val n = x.length()
    f2jBlas.daxpy(n, a, x.values(), 1, y.values(), 1)
  }

  /**
    * y += a * x
    */
  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values()
    val xIndices = x.indices()
    val yValues = y.values()
    val nnz = xIndices.length

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  /**
    * y := alpha*A*x + beta*y
    *
    * @param n The order of the n by n matrix A.
    * @param A The upper triangular part of A in a [[DenseVector]] (column major).
    * @param x The [[DenseVector]] transformed by A.
    * @param y The [[DenseVector]] to be modified in place.
    */
  def dspmv(
             n: Int,
             alpha: Double,
             A: DenseVector,
             x: DenseVector,
             beta: Double,
             y: DenseVector): Unit = {
    f2jBlas.dspmv("U", n, alpha, A.values(), x.values(), 1, beta, y.values(), 1)
  }

  /**
    * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's ?SPR.
    *
    * @param U the upper triangular part of the matrix in a [[DenseVector]](column major)
    */
  def spr(alpha: Double, v: VectorBase, U: DenseVector): Unit = {
    spr(alpha, v, U.values())
  }

  /**
    * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's ?SPR.
    *
    * @param U the upper triangular part of the matrix packed in an array (column major)
    */
  def spr(alpha: Double, v: VectorBase, U: Array[Double]): Unit = {
    val n = v.length()
    v match {
      case dv: DenseVector =>
        nativeBlas.dspr("U", n, alpha, dv.values(), 1, U)
      case sv: SparseVector =>
        val size = sv.length()
        val indices = sv.indices()
        val values = sv.values()
        val nnz = indices.length
        var colStartIdx = 0
        var prevCol = 0
        var col = 0
        var j = 0
        var i = 0
        var av = 0.0
        while (j < nnz) {
          col = indices(j)
          // Skip empty columns.
          colStartIdx += (col - prevCol) * (col + prevCol + 1) / 2
          col = indices(j)
          av = alpha * values(j)
          i = 0
          while (i <= j) {
            U(colStartIdx + indices(i)) += av * values(i)
            i += 1
          }
          j += 1
          prevCol = col
        }
    }
  }


  /**
    * x = a * x
    */
  def scal(a: Double, x: VectorBase): Unit = {
    x match {
      case sx: SparseVector =>
        f2jBlas.dscal(sx.values().length, a, sx.values(), 1)
      case dx: DenseVector =>
        f2jBlas.dscal(dx.length(), a, dx.values(), 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

}
