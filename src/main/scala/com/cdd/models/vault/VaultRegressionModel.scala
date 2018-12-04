package com.cdd.models.vault

import java.io.{File, FileWriter}

import com.cdd.models.datatable.{DataTable, DataTableModelUtil}
import com.cdd.models.pipeline.{BestRegressionEstimators, RegressionEvaluator, RegressorValidation}
import com.cdd.models.utils.Util
import com.opencsv.CSVWriter
import org.apache.commons.lang3.StringUtils

object VaultRegressionModel  {

  class DataTableTooLargeForRegression(message:String="Data table too large for regression") extends Exception(message)

  def saveRegressionResults(name: String, resultsDir: File, regressionResults: Vector[RegressorValidation],
                            fileNamePrefix: String = "regression_"): Unit = {
    if (regressionResults.isEmpty) {
      return
    }

    if (!resultsDir.exists()) {
      throw new RuntimeException(s"Results directory $resultsDir does not exist")
    }
    val resultsFile = new File(resultsDir, s"${fileNamePrefix}results.csv")
    Util.using(new FileWriter(resultsFile)) { out =>
      val csvWriter = new CSVWriter(out)
      csvWriter.writeNext(Array("AssayID", "Field", "Category", "Method", "Metric", "Correlation", "Count"))
      regressionResults.foreach { result =>
        csvWriter.writeNext(Array(name, result.label, result.category, result.estimator.getEstimatorName(),
          result.metric.toString, result.correlation.toString, result.dataSplit.getPredictDt().length.toString))
      }
    }

    val plotFile = new File(resultsDir, s"${fileNamePrefix}plot.png")
    regressionPlots(regressionResults, plotFile.getAbsolutePath)
  }

  def regressionPlots(regressionResults: Vector[RegressorValidation], fileName: String, names: Vector[String] = null, cols: Int = 3): Unit = {
    val evaluator = new RegressionEvaluator().setMetric("rmse")
    val charts = regressionResults.zipWithIndex.map { case (result, i) =>
      val name = if (names == null) "" else names(i)
      val title = StringUtils.abbreviate(s"$name ${result.estimator.getParameter(result.estimator.estimatorName)} ${result.label}".trim, 60)
      evaluator.chart(result.dataSplit.getPredictDt(), title)
    }
    Util.gridPlot(fileName, charts, cols)
  }

  private def applyRegressors(dataTable: DataTable, dataCategory: String, dataLabel:String="label"): Vector[RegressorValidation] = {
    val evaluator = new RegressionEvaluator().setMetric("rmse")
    val dt = DataTableModelUtil.selectTable(dataTable, labelName=dataLabel)
    if (dt.length > 50000) {
      throw new DataTableTooLargeForRegression(s"Data table size too large for regression ${dt.length}")
    }

    val regressors = new BestRegressionEstimators(evaluator = evaluator)
    val regressionResults = regressors.fitDataTable(dataLabel, dataCategory, dt)
    regressionResults
  }
}



class VaultRegressionModel(val dataTable: DataTable, val dataCategory: String = "raw")  {

  def applyRegressors(applyLog: Boolean = false, dataLabel:String="label"): Vector[RegressorValidation] = {
    val results = VaultRegressionModel.applyRegressors(dataTable, dataCategory, dataLabel)
    if (applyLog) {
      applyLogRegressors() match {
        case Some(logResults) => results ++ logResults
        case None => results
      }
    }
    else {
      results
    }
  }

  def applyLogRegressors(): Option[Vector[RegressorValidation]] = {
    val logDtOption = DataTableModelUtil.toLogTable(dataTable)
    logDtOption match {
      case Some(logDt) =>
        val results = VaultRegressionModel.applyRegressors(logDt, s"log_$dataCategory")
        Some(results)
      case _ => None
    }
  }

}
