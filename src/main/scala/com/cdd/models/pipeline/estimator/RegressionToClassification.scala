package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline.{HasLabelColumn, HasPredictionColumn, Identifiable}
import com.cdd.models.utils.HasLogging

class RegressionToClassification(override val uid: String)  extends HasPredictionColumn with HasLabelColumn with HasLogging{
  def this() = this(Identifiable.randomUID("classificationToRegression"))


  def toClassification(dataTable: DataTable, cutoff: Double, greaterBetter: Boolean,
                       minPositiveCount:Int=1, minPositiveProportion:Double=0.0): Option[DataTable] = {
    def toNewLabel(label: Double) = {
      if (greaterBetter) {
        if (label >= cutoff) 1.0 else 0.0
      } else {
        if (label <= cutoff) 1.0 else 0.0
      }
    }

    val labelColumnName = getParameter(labelColumn)
    val predictionColumnName = getParameter(predictionColumn)
    assert(dataTable.hasColumnWithName(labelColumnName))
    assert(dataTable.hasColumnWithName(predictionColumnName))

    val newColumnNames = dataTable.columns.map { c =>
      c.title match {
        case `labelColumnName` => labelColumnName -> "regressionLabel"
        case `predictionColumnName` => predictionColumnName -> "probability"
        case t => t -> t
      }
    }
    val dt = dataTable.selectAs(newColumnNames: _*)
    val regressionLabels = dataTable.column(labelColumnName).toDoubles()
    val regressionPredictions = dataTable.column(predictionColumnName).toDoubles()
    val newLabels = regressionLabels.map(rl =>
      toNewLabel(rl)
    )
    val nPositive = newLabels.count(_ == 1.0)
    if (nPositive < minPositiveCount) {
      logger.warn(s"regressionToClassification: number of positive labels $nPositive less that required $minPositiveCount")
      return None
    }
    val positiveProportion = nPositive.toDouble / regressionLabels.length.toDouble
    if (positiveProportion < minPositiveProportion) {
      logger.warn(s"regressionToClassification: positive proportion $positiveProportion less that required $minPositiveProportion")
      return None
    }
    val newPredictions = regressionPredictions.map(rl =>
      toNewLabel(rl)
    )

    val classificationDt = dt.addColumn(DataTableColumn.fromVector[Double](labelColumnName, newLabels))
      .addColumn(DataTableColumn.fromVector[Double](predictionColumnName, newPredictions))
    Some(classificationDt)
  }


}
