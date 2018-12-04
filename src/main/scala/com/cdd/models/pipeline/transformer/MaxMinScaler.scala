package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable._
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.transformer.StandardScaler.summaryStatisticsForFeatures
import com.cdd.models.utils.SummaryStatistics

private class MaxMinInformation(val featureMax: Double, val featureMin: Double)

trait HasMaxMinScalerParams extends HasParameters {

  val max = new Parameter[Double](this, "max", "Maximum Scale Value")

  def setMax(value: Double): this.type = setParameter(max, value)

  setDefaultParameter(max, 1.0)
  val min = new Parameter[Double](this, "min", "Maximum Scale Value")

  def setMin(value: Double): this.type = setParameter(min, value)

  setDefaultParameter(min, 0.0)

}

class MaxMinScaler(override val uid: String) extends FeatureMapperEstimator[MaxMinScalerModel] with HasMaxMinScalerParams {

  def this() = this(Identifiable.randomUID("feature_range"))

  setDefaultParameter(outputColumn, "scaledFeatures")

  override def fit(dataTable: DataTable): MaxMinScalerModel = {

    val features = dataTable.column(getParameter(featuresColumn)).toVector()
    val stats = summaryStatisticsForFeatures(features)

    copyParameterValues(new MaxMinScalerModel(uid, stats))
  }
}

class MaxMinScalerModel(override val uid: String, private val statistics: Map[Int, SummaryStatistics]) extends FeatureMappingModel[MaxMinScalerModel] with HasMaxMinScalerParams {

  lazy val maxVal = getParameter(this.max)
  lazy val minVal = getParameter(this.min)

  override def transformRow(features: VectorBase): VectorBase = {
    features match {
      case features: DenseVector =>
        val newValues =
        features.toArray().zipWithIndex.map { case (v, i) =>
            newValue(i, v)
        }
        Vectors.Dense(newValues)
      case features: SparseVector =>
        require(minVal == 0)
        val newValues =
          features.values().zip(features.indices()).map { case (v, i) =>
            newValue(i, v)
          }
        Vectors.Sparse(features.indices(), newValues, features.length())
      case _ => throw new IllegalArgumentException("Unknown double vector type")
    }
  }

  private def newValue(index: Int, value: Double): Double = {
    val s = statistics(index)
    var newValue = if (s.max == s.min) 0.0 else (value - s.min) / (s.max - s.min)
    (newValue * (maxVal - minVal)) + minVal
  }

}