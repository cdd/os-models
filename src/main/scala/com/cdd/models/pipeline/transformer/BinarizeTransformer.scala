package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable._
import com.cdd.models.pipeline.{FeatureMappingRowTransformer, HasOutputColumn, Identifiable, Transformer}


class BinarizeTransformer(override val uid: String) extends FeatureMappingRowTransformer with HasOutputColumn {
  def this() = this(Identifiable.randomUID("binarize_tf"))

  setParameter(outputColumn, "binaryFeatures")

  override def transformRow(features: VectorBase): VectorBase = {
    features match {
      case features: DenseVector =>
        val newValues= features.toArray.map { v =>
          if (v == 0.0) 0.0 else 1.0
        }
        Vectors.Dense(newValues)
      case features: SparseVector =>
        val newValues = features.values().map { v => if (v > 0) 1.0 else 0.0 }
        Vectors.Sparse(features.indices(), newValues, features.length())
      case _ => throw new IllegalArgumentException("Unknown double vector type")
    }
  }
}
