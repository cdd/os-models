package com.cdd.models.pipeline.transformer


import com.cdd.models.datatable._
import com.cdd.models.pipeline._
import com.cdd.models.utils.SummaryStatistics

import scala.collection.mutable


trait HasStandardScalerParams extends HasParameters {

  val center = new Parameter[Boolean](this, "center", "Center round the mean value")

  def setCenter(value: Boolean): this.type = setParameter(center, value)

  setDefaultParameter(center, true)

  val scale = new Parameter[Boolean](this, "scale", "Scale by the standard deviation")

  def setScale(value: Boolean): this.type = setParameter(scale, value)

  setDefaultParameter(scale, true)

}

object StandardScaler {

  def summaryStatisticsArrayForFeatures(features: Vector[VectorBase]): Vector[SummaryStatistics] = {
    summaryStatisticsForFeatures(features).toSeq.sortBy(_._1).map(_._2).toVector
  }

  def summaryStatisticsForFeatures(features: Vector[VectorBase]): Map[Int, SummaryStatistics] = {
    val stats = mutable.Map[Int, SummaryStatistics]()
    var sparseFeatures = false
    var length = 0

    features.foreach {
      case instanceFeatures: DenseVector =>
        require(!sparseFeatures)
        instanceFeatures.toArray().zipWithIndex.foreach { case (v, i) =>
          stats.getOrElseUpdate(i, new SummaryStatistics()).addValue(v)
          assert(stats(i).max >= v)
        }
      case instanceFeatures: SparseVector =>
        sparseFeatures = true

        instanceFeatures.values().zip(instanceFeatures.indices()).foreach { case (v, i) =>
          stats.getOrElseUpdate(i, new SummaryStatistics()).addValue(v)
        }

      case _ => throw new IllegalArgumentException("Unknown features column type")
    }

    if (sparseFeatures) {
      require(stats.size == features(0).length())
      val length = features.length.toLong
      stats.foreach { case (_, s) => s.addZeros(length - s.no) }
    }

    stats.toMap
  }
}

class StandardScaler(override val uid: String) extends FeatureMapperEstimator[StandardScalerModel] with HasStandardScalerParams {

  import StandardScaler.summaryStatisticsForFeatures

  def this() = this(Identifiable.randomUID("feature_norm"))

  setDefaultParameter(outputColumn, "scaledFeatures")

  override def fit(dataTable: DataTable): StandardScalerModel = {

    val features = dataTable.column(getParameter(featuresColumn)).toVector()
    val stats = summaryStatisticsForFeatures(features)

    copyParameterValues(new StandardScalerModel(uid, stats))
  }
}


class StandardScalerModel(override val uid: String, private val statistics: Map[Int, SummaryStatistics]) extends FeatureMappingModel[StandardScalerModel] with HasStandardScalerParams {
  lazy val centerVal = getParameter(this.center)
  lazy val scaleVal = getParameter(this.scale)

  override def transformRow(features: VectorBase): VectorBase = {

    features match {
      case features: DenseVector =>
        val newValues =
          features.toArray().zipWithIndex.map { case (v, i) =>
            newValue(i, v)
          }
        Vectors.Dense(newValues)
      case features: SparseVector => {
        require(!centerVal)
        val newValues =
          features.values().zip(features.indices()).map { case (v, i) =>
            newValue(i, v)
          }
        Vectors.Sparse(features.indices(), newValues, features.length())
      }
      case _ => throw new IllegalArgumentException("Unknown double vector type")
    }
  }

  private def newValue(index: Int, value: Double): Double = {
    val s = statistics(index)
    var newValue = value
    if (centerVal) newValue = newValue - s.mean()
    val sd = s.standardDeviation()
    if (scaleVal && !sd.isNaN && sd > 0) newValue = newValue / sd
    newValue
  }
}