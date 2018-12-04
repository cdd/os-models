package com.cdd.models.vault

import java.io.{File, FileWriter}

import com.cdd.models.datatable.{DataTable, DataTableModelUtil}
import com.cdd.models.pipeline.{BestClassificationEstimators, ClassificationEvaluator, ClassifierValidation}
import com.cdd.models.utils.Util
import com.opencsv.CSVWriter
import org.apache.commons.lang3.StringUtils

object VaultClassificationModel {

  def saveClassificationResults(name:String, resultsDir: File, classificationResults: Vector[ClassifierValidation],
                                fileNamePrefix:String="classification_"):Unit = {
    if (classificationResults.isEmpty) {
      return
    }

    if (!resultsDir.exists()) {
        throw new RuntimeException(s"Results directory $resultsDir does not exist")
    }
    val resultsFile = new File(resultsDir, s"${fileNamePrefix}results.csv")
    Util.using(new FileWriter(resultsFile)) { out =>
      val csvWriter = new CSVWriter(out)
      csvWriter.writeNext(Array("AssayID", "Field", "Category", "Method", "Metric", "Correlation", "Count"))
      classificationResults.foreach { result =>
        csvWriter.writeNext(Array(name, result.label, result.category, result.estimator.getEstimatorName(),
          result.metric.toString, result.dataSplit.getPredictDt().length.toString))
      }
    }

    val plotFile = new File(resultsDir, s"${fileNamePrefix}plot.png")
    classificationPlots(classificationResults, plotFile.getAbsolutePath)
  }

  def classificationPlots(ClassificationResults: Vector[ClassifierValidation], fileName: String, names: Vector[String] = null, cols:Int=3): Unit = {
    val evaluator = new ClassificationEvaluator().setMetric("rmse")
    val charts = ClassificationResults.zipWithIndex.map { case(result, i) =>
      val name = if (names == null) "" else names(i)
      val title = StringUtils.abbreviate(s"$name ${result.estimator.getParameter(result.estimator.estimatorName)} ${result.label}".trim, 60)
      evaluator.chart(result.dataSplit.getPredictDt(), title)
    }
    Util.gridPlot(fileName, charts, cols)
  }
}

class VaultClassificationModel(val dataTable:DataTable, val dataCategory:String="raw") {

  def applyClassifiers(labelColumnName:String = "label"): Vector[ClassifierValidation] = {
    val evaluator = new ClassificationEvaluator().setMetric("rocAuc")
    val dt = DataTableModelUtil.selectTable(dataTable, labelColumnName)
    if (dt.length > 500000) {
      throw new RuntimeException(s"Data table size too large for Classification ${dt.length}")
    }

    val classifiers = new BestClassificationEstimators(evaluator = evaluator)
    val classificationResults = classifiers.fitDataTable(labelColumnName, dataCategory,  dt)
    classificationResults
  }
}
