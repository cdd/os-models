package com.cdd.models.pipeline.tuning

import java.io.File

import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline._
import com.cdd.models.utils.{HasLogging, Util}

import scala.collection.mutable.ArrayBuffer


case class CrossValidationMetrics(bestIndex: Int, avgMetrics: Array[Double], splitMetrics: Array[Array[Double]],
                                  parameterGrid: ParameterGrid, evaluator: Evaluator)
  extends HasLogging {

  def logToFile(fileName: String, name: String = "Unknown system", methodName: String = "Unknown method"): Unit = {
    require(fileName.endsWith(".csv"))

    Util.runWithLockFile(fileName + ".lck", () => {
      val file = new File(fileName)
      val exists = file.exists()
      if (exists) {
        val currentResults = DataTable.loadFile(fileName)
        val currentNames = currentResults.column("NAME").uniqueValues().asInstanceOf[Vector[String]]
        if (currentNames.find(_ == name) != None) {
          logger.warn(s"Results for ${name} already present in ${fileName}")
          return
        }
      }
      val dataTable = toDataTable(fileName, name, methodName)
      dataTable.exportToFile(fileName, append = true, writeHeaders = !exists)
    })
  }

  def getMetric(parameterMap: ParameterMap): Option[Double] = {
    val metric = (parameterGrid.parameterMap zip avgMetrics).find { case (map, metric) =>
      map == parameterMap
    }
    metric match {
      case Some((_, m)) => Some(m)
      case _ => None
    }
  }

  def toDataTable(fileName: String, name: String = "Unknown system", methodName: String = "Unknown method"): DataTable = {

    val parameters = parameterGrid.parameters
    val parameterNames = parameters.map(_.name)
    val nFolds = splitMetrics(0).length
    val nColumns = 2 + parameters.length + 1 + nFolds
    val nRows = parameterGrid.parameterMap.length
    val data = (0 until nColumns).map { _ => new Array[Any](nRows) }.toVector
    val evalName = evaluator.getParameter(evaluator.metric)

    val columnTitles = new ArrayBuffer[String]
    columnTitles.append("NAME")
    columnTitles.append("METHOD")
    columnTitles ++= parameterNames
    columnTitles.append(evalName)
    for (foldNo <- 1 to nFolds)
      columnTitles.append(s"${evalName}_${foldNo}")

    parameterGrid.parameterMap.zipWithIndex zip avgMetrics zip splitMetrics foreach {
      case (((paramMap, rowNo), avgMetric), allMetrics) =>

        var colNo = 0
        data(colNo)(rowNo) = name
        colNo += 1
        data(colNo)(rowNo) = methodName
        colNo += 1

        parameters.foreach { parameter =>
          val value = paramMap.get(parameter).get
          data(colNo)(rowNo) = value
          colNo += 1
        }

        data(colNo)(rowNo) = avgMetric
        colNo += 1

        allMetrics.foreach { m =>
          data(colNo)(rowNo) = m
          colNo += 1
        }
    }

    val columns = data.zip(columnTitles) map { case (d, t) => new DataTableColumn[Any](t, d.toVector.map(Some(_))) }
    new DataTable(columns)
  }
}

class HyperparameterOptimizer(override val uid: String) extends HasParameters with HasLogging {

  def this() = this(Identifiable.randomUID("optimizer"))

  val evaluator = new Parameter[Evaluator](this, "evaluator", "Cross validation evaluator")

  def setEvaluator(value: Evaluator) = setParameter(evaluator, value)

  val estimator = new Parameter[Estimator[_, _]](this, "estimator", "Estimator to evaluate")

  def setEstimator(value: Estimator[_, _]) = setParameter(estimator, value)

  val numFolds = new Parameter[Int](this, "numFolds", "Number of cross validation folds")

  def setNumFolds(value: Int) = setParameter(numFolds, value)

  setDefaultParameter(numFolds, 3)

  val parameterGrid = new Parameter[ParameterGrid](this, "parameterGrid", "Grid or parameters to optimize")

  def setParameterGrid(value: ParameterGrid) = setParameter(parameterGrid, value)

  def optimizeParameters(dataTable: DataTable): CrossValidationMetrics = {
    val est = getParameter(estimator)
    val eval = getParameter(evaluator)
    val paramGrid = getParameter(parameterGrid)
    val nModels = paramGrid.parameterMap.length
    val nFolds = getParameter(numFolds)
    val metrics = new Array[Double](nModels)
    val splitMetrics = Array.ofDim[Double](nModels, nFolds)

    (0 until nFolds).foreach { foldNo =>
      val (training, validation) = dataTable.trainValidateFold(foldNo, nFolds)

      paramGrid.parameterMap.zipWithIndex.foreach { case (pm, pmIndex) =>
        logger.warn(s"Training fold ${foldNo + 1} parameter map ${pmIndex + 1}.")
        var model = est.fit(training, pm).asInstanceOf[Model[_, _]]
        val metric = eval.evaluate(model.transform(validation))
        logger.warn(s"Got metric $metric for model trained with ${pm} fold ${foldNo + 1}.")
        splitMetrics(pmIndex)(foldNo) = metric
        metrics(pmIndex) += metric
        logger.warn(s"Memory ${Runtime.getRuntime().totalMemory() / (1024 * 1024)}M total ${Runtime.getRuntime().freeMemory() / (1024 * 1024)}M free")
      }
    }

    metrics.zipWithIndex.foreach { case (m, index) => metrics(index) = m / nFolds.toDouble }
    logger.warn(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logger.warn(s"Best set of parameters:\n${paramGrid.parameterMap(bestIndex)}")
    logger.warn(s"Best cross-validation metric: $bestMetric.")

    CrossValidationMetrics(bestIndex, metrics, splitMetrics, paramGrid, eval)
  }


}
