package com.cdd.models.pipeline.estimator

import breeze.linalg
import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline._
import com.cdd.models.utils.HasLogging
import com.cdd.models.pipeline.ClassificationEvaluator.ClassificationStats
import com.cdd.models.pipeline.estimator.OptimizeClassificationPredictions.OptimizeClassificationPredictionsResults

object OptimizeClassificationPredictions {
  class OptimizeClassificationPredictionsResults
  (val dataTable: DataTable, val cutoffsAndMetrics:Vector[(Double, Double)],
   val bestCutoff:Double, val bestMetric:Double)
}

/**
  * Given a metricFunction (that scores classification statistics), recalculates the output labels by choosing an
  * optimal threshold on the classifier probability..
  *
  * @param uid
  */
class OptimizeClassificationPredictions(override val uid: String) extends HasPredictionColumn with  HasProbabilityColumn
  with HasLabelColumn with  HasLogging {

  def this() = this(Identifiable.randomUID("optimizeClassificationPredictions"))

  def optimizeClassificationPrediction(dataTable: DataTable, metricFunction: ClassificationStats => Double,
                                       nSteps:Int = 100) : OptimizeClassificationPredictionsResults = {

    val labelColumnName = getParameter(labelColumn)
    val predictionColumnName = getParameter(predictionColumn)
    val probabilityColumnName = getParameter(probabilityColumn)

    val evaluator = copyParameterValues(new ClassificationEvaluator())

    val newColumnNames = dataTable.columns.map { c =>
      c.title match {
        case `predictionColumnName` => predictionColumnName -> "originalPrediction"
        case t => t -> t
      }
    }

    val dt = dataTable.selectAs(newColumnNames:_*)
    val probabilities = dataTable.column(probabilityColumnName).toDoubles()
    val labels = dataTable.column(labelColumnName).toDoubles()
    val min = probabilities.min
    val max = probabilities.max
    val range = linalg.linspace(min, max, nSteps).toScalaVector()
    var bestMetric = -Double.MaxValue
    var bestPredictions:Vector[Double] = null
    var bestCutoff:Double = 0.0
    val metricRange = range.map { cutoff =>
      val predictions = probabilities.map { p => if (p>=cutoff) 1.0 else 0.0}
      val stats = evaluator.statistics(labels, predictions)
      val metric = metricFunction(stats)
      if (metric > bestMetric) {
        bestMetric = metric
        bestCutoff = cutoff
        bestPredictions = predictions
      }
      (cutoff, metric)
    }

    val resultDt = dt.addColumn(DataTableColumn.fromVector(predictionColumnName, bestPredictions))
    new OptimizeClassificationPredictionsResults(resultDt, metricRange, bestCutoff, bestMetric)
  }

}
