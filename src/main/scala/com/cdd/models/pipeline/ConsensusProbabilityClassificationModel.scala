package com.cdd.models.pipeline

import com.cdd.models.datatable.{DataTable, DataTableColumn, VectorBase}
import com.cdd.models.pipeline.estimator.{MajorityVotingClassificationModel, MajorityVotingClassificationModelBase, MajorityVotingClassifier}
import org.apache.commons.math3.stat.StatUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

class ConsensusProbabilityClassifier(override val uid: String) extends MajorityVotingClassifier(uid) {
  def this() = this(Identifiable.randomUID("cons_probability"))


}

@SerialVersionUID(-4343357377946754656L)
class ConsensusProbabilityClassificationModel(override val uid: String,
                                              override val models: Vector[PipelineModel],
                                              val weights: Vector[Double])
  extends PredictionModelWithProbabilities[ConsensusProbabilityClassificationModel]
    with HasFeaturesAndLabelParameters
    with MajorityVotingClassificationModelBase {

  def this(models:Vector[PipelineModel], weights:Vector[Double]) =
    this(Identifiable.randomUID("cons_probability"), models, weights)

  require(weights.length== models.length)

  override def transform(dataTable: DataTable): DataTable = {
    val dataTables = models.map { m =>
      require(m.isInstanceOf[PipelineModel])
      m.transform(dataTable)
    }
    val probabilities = dataTables.zipWithIndex.map { case(dt, i) =>
      // assume probability column has not been renamed from default
      require(dt.hasColumnWithName("probability"))
      val probs = dt.column("probability").toDoubles()
      if (probs.length > 10) {
        val stats = new DescriptiveStatistics(probs.toArray)
        logger.info(s"Model $i max ${stats.getMax} mean ${stats.getMean} min ${stats.getMin} sd ${stats.getStandardDeviation}")
      }
      probs
    }
    val combinedProbabilities = Vector.range(0, dataTable.length).map { row =>
      val rowProbabilities = probabilities.map( v => v(row))
      rowProbabilities.zip(weights).map { case(p, w) =>
        p*w
      } sum

    }

    val newColumn = DataTableColumn.fromVector(getParameter(probabilityColumn), combinedProbabilities)
    dataTable.addColumn(newColumn)
  }

  override def transformRow(features: VectorBase): (Double, Double) = {
      throw new NotImplementedError()
  }

  def getFeaturesColumns():Vector[String]  = {
    models.map(_.getFeaturesColumn())
  }

  override def getFeaturesColumn(): String = {
    getFeaturesColumns().mkString("|")
  }

  def modelIds() : Vector[String] = {
    models.map(_.getEstimatorId())
  }
}
