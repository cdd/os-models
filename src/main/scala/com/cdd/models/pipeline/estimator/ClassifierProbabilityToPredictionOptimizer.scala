package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline.ClassificationEvaluator.ClassificationStats
import com.cdd.models.pipeline._
import com.cdd.models.utils.HasLogging


trait ClassifierProbabilityToPredictionOptimizerParameters extends HasLabelColumn
  with HasProbabilityColumn with HasPredictionColumn {

  val optimizerFunction = new Parameter[String](this, "function", "Target function for optimizer (one of f1 or ba)")
  setDefaultParameter(optimizerFunction, "f1")

  def setOptimizerFunction(value: String): this.type = setParameter(optimizerFunction, value)

  protected def evaluateStatistics(stats: ClassificationStats): Double = {
    getParameter(optimizerFunction) match {
      case "f1" => stats.f1
      case "ba" => stats.ba
      case f => throw new IllegalArgumentException(s"Unknown optimizer function $f")
    }
  }

  protected def buildEvaluator(): ClassificationEvaluator = {
    new ClassificationEvaluator()
      .setLabelColumn(getParameter(labelColumn))
      .setPredictionColumn(getParameter(predictionColumn))
  }
}

class ClassifierProbabilityToPredictionOptimizer(override val uid: String)
  extends EstimatorBase[ClassifierProbabilityToPredictionTransform, Double, Double]
    with ClassifierProbabilityToPredictionOptimizerParameters with HasLogging {
  def this() = this(Identifiable.randomUID("class_thresh"))

  /**
    * Fits a model to the input data.
    */
  override def fit(dataTable: DataTable): ClassifierProbabilityToPredictionTransform = {
    val probabilityCol = getParameter(probabilityColumn)
    val uniqueProbabilities = dataTable.column(probabilityCol).toDoubles().distinct.sorted
    val thresholdsAndScores = uniqueProbabilities.sliding(2).map { case Vector(p1, p2) =>
      val threshold = (p1 + p2) / 2.0
      val score = evaluateThreshold(threshold, dataTable)
      (threshold, score)
    }.toVector
    val bestThresholdAndScore = thresholdsAndScores.maxBy(_._2)
    val bestThreshold = bestThresholdAndScore._1

    val model = copyParameterValues(new ClassifierProbabilityToPredictionTransform(uid, bestThreshold))

    val testResults = model.transform(dataTable)

    val evaluator = buildEvaluator()
    val testStats = evaluator.statistics(testResults)
    assert(evaluateStatistics(testStats) == bestThresholdAndScore._2)

    if (dataTable.hasColumnWithName(getParameter(predictionColumn))) {
      val initialScore = evaluateStatistics(evaluator.statistics(dataTable))
      logger.debug(s"Training optimized input score from $initialScore to ${bestThresholdAndScore._2}")
    } else {
      logger.debug(s"Training optimized score ${bestThresholdAndScore._2}")
    }
    model
  }

  private def evaluateThreshold(threshold: Double, dataTable: DataTable) = {
    var tp = 0
    var fp = 0
    var tn = 0
    var fn = 0

    dataTable.column(getParameter(probabilityColumn)).toDoubles().zip(dataTable.column(getParameter(labelColumn)).toDoubles())
      .foreach { case (prob, label) =>
        require(label == 0.0 || label == 1.0)
        val prediction = if (prob > threshold) 1.0 else 0.0
        (label, prediction) match {
          case (1.0, 1.0) => tp += 1
          case (0.0, 1.0) => fp += 1
          case (0.0, 0.0) => tn += 1
          case (1.0, 0.0) => fn += 1
          case _ => throw new IllegalArgumentException
        }
      }

    val stats = new ClassificationStats(tp = tp, fp = fp, tn = tn, fn = fn)
    evaluateStatistics(stats)
  }

}

@SerialVersionUID(1000L)
class ClassifierProbabilityToPredictionTransform(override val uid: String, val threshold: Double)
  extends ModelBase[ClassifierProbabilityToPredictionTransform, Double, Double]
    with ClassifierProbabilityToPredictionOptimizerParameters with HasLogging {


  override def transformRow(probability: Double): Double = {
    if (probability > threshold) 1.0 else 0.0
  }

  override def transform(dataTable: DataTable): DataTable = {
    val allProbabilities = dataTable.column(getParameter(probabilityColumn)).toDoubles()
    val predictions = allProbabilities.map {
      transformRow
    }

    val predictionCol = getParameter(predictionColumn)
    val startTable =
      if (dataTable.hasColumnWithName(predictionCol)) {
        logger.debug(s"Replacing existing prediction column [$predictionCol]")
        dataTable.removeColumns(predictionCol)
      } else {
        dataTable
      }

    val transformedTable = startTable.addColumn(DataTableColumn.fromVector(predictionCol, predictions))

    if (transformedTable.hasColumnWithName("label")) {
      val evaluator = buildEvaluator()
      val finalScore = evaluateStatistics(evaluator.statistics(transformedTable))
      if (dataTable.hasColumnWithName(predictionCol)) {
        val initialScore = evaluateStatistics(evaluator.statistics(dataTable))
        logger.debug(s"Model final score from $initialScore to $finalScore")
      } else {
        logger.debug(s"Model final score $finalScore")
      }
    }
    transformedTable
  }

}