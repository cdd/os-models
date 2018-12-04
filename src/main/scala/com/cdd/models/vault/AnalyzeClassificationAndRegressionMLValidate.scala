package com.cdd.models.vault

import java.io.File

import com.cdd.models.pipeline.{ClassificationEvaluator, RegressionEvaluator}
import com.cdd.models.universalmetric.{HistogramDataType, NormalizedHistogram}
import com.cdd.models.utils.{HasLogging, HasSwingCharts, Util}
import com.cdd.models.vault.AnalyzeClassificationAndRegressionMLValidate.ResultSummary
import com.cdd.models.vault.DatasetCategory.DatasetCategory
import scalafx.application
import scalafx.application.JFXApp
import scalafx.event.ActionEvent
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.control.{Button, Label, ScrollPane, TabPane}
import scalafx.scene.layout._
import scalafx.stage.Stage
import scalafx.Includes._
import scalafx.scene.control._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object AnalyzeClassificationAndRegressionMLValidateApp extends JFXApp with HasLogging {

  import AnalyzeClassificationAndRegressionMLValidate._

  private val files =
    if (parameters.unnamed.nonEmpty)
      parameters.unnamed
    else
      Vector(
        "data/vault/protocol_molecule_features/TrypanosomeValidation.txt",
        "data/vault/protocol_molecule_features/TbValidation.txt",
        "data/vault/protocol_molecule_features/MalariaValidation.txt"
      )
  val categoriesAndResults = readCategoriesAndResults(files)

  stage = new application.JFXApp.PrimaryStage {
    title = "Classification and Regression Viewer"
  }
  stage.scene = new ValidateSummaryScene(categoriesAndResults)

  def readCategoriesAndResults(fileNames: Seq[String]): Vector[ResultSummary] = {
    val data = ArrayBuffer[ResultSummary]()
    for (fileName <- fileNames) {
      val file = Util.getProjectFilePath(fileName)
      if (!file.exists) {
        logger.warn(s"Unable to open ${file.getAbsoluteFile}")
      }
      else {
        val nameMatcher = """processing vault data table (.*)$""".r
        val categoryMatcher = """^Category: (\w+) Result File: '(.*)'$""".r
        var currentName: String = null
        Util.using(Source.fromFile(file)) { source =>
          for (line <- source.getLines()) {
            val categoryMatch = categoryMatcher.findFirstMatchIn(line)
            categoryMatch match {
              case Some(m) =>
                val category = DatasetCategory.withName(m.group(1))
                val resultFile = new File(m.group(2))
                require(resultFile.exists)
                require(currentName != null)
                data.append(new ResultSummary(category, currentName, resultFile))
              case None =>
            }
            val nameMatch = nameMatcher.findFirstMatchIn(line)
            nameMatch match {
              case Some(m) => currentName = m.group(1)
              case None =>
            }
          }
        }
      }
    }
    data.toVector
  }
}

class ValidateSummaryScene(val summaries: Vector[SummaryItem]) extends Scene(800, 600) with HasLogging with SummarySceneBase {
  override val nCols: Int = 1
  override val abbrevLength: Int = 300

  drawScrollPane()

  def redraw(): Unit = {
    drawScrollPane()
  }

  private def drawScrollPane() = {

    val tabs = DatasetCategory.values.toSeq.flatMap(drawTab)
    val tabbedPane = new TabPane() {

      hgrow = Priority.Always
      vgrow = Priority.Always
      id = "source-tabs"
      //tabs = tabs.toSeq
    }
    tabbedPane.tabs = tabs
    val buttonPane = new HBox()
    val contentPane = new VBox()
    contentPane.children.add(buttonPane)
    contentPane.children.add(tabbedPane)

    val scrollPane = new ScrollPane with SceneScrollPane {
      bindSize(ValidateSummaryScene.this)
    }
    content = scrollPane
    scrollPane.content = contentPane
  }

  override def showSummary(item: SummaryItem): Unit = {
    val summary = item.asInstanceOf[ResultSummary]
    val scene = new ValidateDatasetSummaryScene(summaries)
    scene.drawScrollPane(summary)

    val stage = new Stage {
      title = s"Validation data"
    }
    stage.scene = scene
    stage.show()
  }
}

object AnalyzeClassificationAndRegressionMLValidate extends JFXApp with HasLogging {

  case class ResultSummary(category: DatasetCategory, dataTableName: String, resultsFile: File) extends SummaryItem {
    override def name(): String = dataTableName
  }

}

class AnalyzeClassificationAndRegressionMLValidate(val results: Vector[ResultSummary]) {

}

class ValidateDatasetSummaryScene(val summaries: Vector[SummaryItem]) extends Scene(800, 600)
  with HasLogging with HasSwingCharts {

  def drawScrollPane(summary: ResultSummary) = {
    val scenePane = drawScene(summary)
    val scrollPane = new ScrollPane with SceneScrollPane {
      bindSize(ValidateDatasetSummaryScene.this)
    }
    content = scrollPane
    scrollPane.content = scenePane
  }

  private def printPair[A](g: GridPane, y: Int, name: String, value: A, x: Int = 0, xSpan: Int = 1) = {
    g.add(new Label(name), x, y)
    g.add(new Label(value.toString), x + 1, y, xSpan, 1)
    y + 1
  }

  private def drawScene(summary: ResultSummary): Pane = {

    val result = Util.deserializeObject[VaultClassificationAndRegressionMLValidateResult](summary.resultsFile.getAbsolutePath)
    assert(result.vaultInformation == summary.name())
    val (category, featureMap) = result.predict()


    new GridPane {
      vgap = 5.0
      hgap = 5.0
      padding = Insets(5.0)

      val regressionDt = result.regression.dataSplit.getPredictDt()
      val classificationDt = result.classification.dataTable

      val regressionEvaluator = new RegressionEvaluator().setMetric("rmse")
      val regressionChart = regressionEvaluator.chart(regressionDt, "Regression", width = 400, height = 400, fontSize = 10, showLine = false)
      val classificationEvaluator = new ClassificationEvaluator().setMetric("rocAuc")
      val classificationChart = classificationEvaluator.chart(classificationDt, width = 400, height = 400, fontSize = 10)
      val stats = classificationEvaluator.statistics(classificationDt)
      val f1 = stats.f1
      val np = stats.tp + stats.fn
      val nPredictP = stats.tp + stats.fp
      val regressionEstimator = result.regression.estimator.getEstimatorName()
      val classificationEstimator = result.classification.regressorValidation.estimator.getEstimatorName()

      val label = result.regression.label
      val dataType = if (label == "p_ic50") HistogramDataType.PIC50 else HistogramDataType.RAW
      val histData = regressionDt.column("label").toDoubles()
      val histogram = new NormalizedHistogram(histData, dataType = dataType, title = label)
      val histogramChart = histogram.plot(400, 400, legend = "Histogram", fontSize = 10)

      var y = 0
      y = printPair(this, y, "Dataset", result.datasetInformation.information, xSpan = 8)
      y = printPair(this, y, "Name", result.vaultInformation, xSpan = 8)
      y = printPair(this, y, "Category", category)
      y = printPair(this, y, "Size", regressionDt.length)
      y = printPair(this, y, "Greater better", result.classification.greaterBetter)
      y = printPair(this, y, "Classification label cut point", result.classification.labelThreshold)
      y = printPair(this, y, "Classification prediction cut point", result.classification.predictionThreshold)
      y = printPair(this, y, "f1", f1)
      y = printPair(this, y, "np", np)
      y = printPair(this, y, "% positive", np.toDouble * 100.0 / regressionDt.length.toDouble)
      y = printPair(this, y, "n predict p", nPredictP)
      //y = printPair(this, y,  "", )
      y = printPair(this, y, "Regression estimator", regressionEstimator)
      y = printPair(this, y, "Classification estimator", classificationEstimator)


      featureMap.foreach { case (k, v) =>
        y = printPair(this, y, k, v)
      }

      val hbox = new HBox() {
        padding = Insets(10)
      }

      hbox.children.add(chartNode(regressionChart))
      hbox.children.add(chartNode(classificationChart))
      hbox.children.add(chartNode(histogramChart))
      add(hbox, 0, y, 8, 1)
      y += 1

      val hbox2 = new HBox() {
        padding = Insets(10)
      }


      val resultSummaries = summaries.map(_.asInstanceOf[ResultSummary])
      val currentIndex = summaries.indexWhere(_ == summary)
      require(currentIndex >= 0)
      if (currentIndex < resultSummaries.length - 2) {
        val nextButton = new Button("Next") {
          onAction = (e: ActionEvent) => {
            drawScrollPane(resultSummaries(currentIndex + 1))
          }
        }
        hbox2.children.add(nextButton)
      }

      if (currentIndex > 0) {
        val prevButton = new Button("Previous") {
          onAction = (e: ActionEvent) => {
            drawScrollPane(resultSummaries(currentIndex - 1))
          }
        }
        hbox2.children.add(prevButton)
      }

      val nextSameOpt = resultSummaries.zipWithIndex.find { case (s, i) =>
        i > currentIndex && s.category == category
      }.map(_._1)
      if (nextSameOpt.isDefined) {
        val nextButton = new Button(s"Next $category") {
          onAction = (e: ActionEvent) => {
            val nextSummary = nextSameOpt.get
            drawScrollPane(nextSummary)
          }
        }
        hbox2.children.add(nextButton)
      }

      add(hbox2, 0, y, 8, 1)
    }

  }

}