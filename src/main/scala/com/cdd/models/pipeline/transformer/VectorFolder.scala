package com.cdd.models.pipeline.transformer


import com.cdd.models.datatable.{DenseVector, SparseVector, VectorBase, Vectors}
import com.cdd.models.pipeline.{FeatureMappingRowTransformer, Identifiable, Parameter}

object VectorFolder {

  private def foldSparse(v: SparseVector, foldSize: Int): VectorBase = {
    if (foldSize >= v.length()) {
      return Vectors.Dense(v.toArray())
    }

    val foldedValues = Array.fill(foldSize) {
      .0
    }

    v.indices.zip(v.values).foreach { case (i, v) =>
      val fold = i % foldSize
      foldedValues(fold) += v
    }

    Vectors.Dense(foldedValues)
  }

  private def foldDense(v: DenseVector, foldSize: Int): VectorBase = {
    if (foldSize >= v.length()) {
      return v
    }

    val foldedValues = Array.fill(foldSize) {
      .0
    }
    v.toArray().zipWithIndex.foreach { case (v, i) =>
      val fold = i % foldSize
      foldedValues(fold) += v
    }

    foldedValues.foreach { v => assert(v >= 0, "Folded value must be positive") }
    Vectors.Dense(foldedValues)
  }

}

@SerialVersionUID(1000L)
class VectorFolder(override val uid: String) extends FeatureMappingRowTransformer with Serializable {

  def this() = this(Identifiable.randomUID("folder"))

  import VectorFolder.{foldSparse, foldDense}

  final val foldSize = new Parameter[Int](this, "foldSize", "Length to fold vector into")

  def setFoldSize(value: Int): this.type = setParameter(foldSize, value)

  setParameter(foldSize, 512)

  setParameter(outputColumn, "foldedFeatures")


  override def transformRow(features: VectorBase): VectorBase = {
    val size = getParameter(foldSize)
    features match {
      case features: DenseVector => foldDense(features, size)
      case features: SparseVector => foldSparse(features, size)
      case _ => throw new IllegalArgumentException("Unknown double vector type")
    }
  }

}
