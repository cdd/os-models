package com.cdd.models.vault

import java.io.File

import com.cdd.models.bae.AssayDirection
import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline.ClassificationEvaluator.ClassificationStats
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.estimator.RegressionToClassification
import com.cdd.models.utils.{HasLogging, HasSwingCharts, Util}
import com.cdd.models.vault.DatasetCategory.DatasetCategory
import javax.swing.SwingUtilities
import org.apache.commons.lang3.StringUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.knowm.xchart.{XChartPanel, XYChart}
import scalafx.Includes._
import scalafx.application
import scalafx.application.JFXApp
import scalafx.embed.swing.SwingNode
import scalafx.event.ActionEvent
import scalafx.geometry.Insets
import scalafx.scene.Scene
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.stage.Stage

import scala.collection.immutable.HashMap
import scala.collection.mutable

object AnalyzeClassificationAndRegression extends JFXApp with HasLogging {

  val summaries = DatasetSummary.summaries

  stage = new application.JFXApp.PrimaryStage {
    title = "Classification and Regression Viewer"
  }
  build()

  def build() = {
    stage.scene = new SummaryScene(summaries)
  }
}

object DatasetCategory extends Enumeration {
  type DatasetCategory = Value
  val Regression, Classification, Both, Bad, Unassigned = Value
}

@SerialVersionUID(1000L)
case class DatasetCategoryKey(datasetInformation: DatasetInformation, labelReadout: String,
                              groupReadoutNamesAndValues: Option[Vector[(String, String)]],
                              commonTerms: Map[String, Any]) extends Serializable

object DatasetSummary extends HasLogging {

  private val classificationSummaryFile = PublicVaultRegressionToMultipleClassificationCutoff.defaultClassificationSummaryFile
  private val regressionSummaryFile = PublicVaultEstimator.regressionSummaryFile

  val summaryFileWithCategories = Util.getProjectFilePath("data/vault/protocol_molecule_features/regression_classification_categories_summary.csv")
  private val dataTableCache = mutable.Map[String, Vector[VaultDataTable]]()
  val infoCategoryDataFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/info_and_categories.obj")
  private var categories = loadCategories()
  val summaries = loadSummaries()

  private def loadCategories() = {
    if (infoCategoryDataFile.exists()) {
      Util.deserializeObject[HashMap[DatasetCategoryKey, DatasetCategory]](infoCategoryDataFile.getAbsolutePath)
    } else {
      HashMap[DatasetCategoryKey, DatasetCategory]()
    }
  }

  def setCategoryForDataset(datasetSummary: DatasetSummary, category: DatasetCategory) = {
    categories += datasetSummary.toDatasetCategoryInformation() -> category
    Util.serializeObject(infoCategoryDataFile.getAbsolutePath, categories)
  }

  def getCategoryForDataset(datasetSummary: DatasetSummary): DatasetCategory.Value = {
    categories.get(datasetSummary.toDatasetCategoryInformation()) match {
      case Some(c) => c
      case None => DatasetCategory.Unassigned
    }
  }

  private def loadSummaries(): Vector[DatasetSummary] = {

    val regressionDataTable = DataTable.loadFile(regressionSummaryFile.getAbsolutePath)
    val classificationDataTable = DataTable.loadFile(classificationSummaryFile.getAbsolutePath)
    require(regressionDataTable.length == classificationDataTable.length)

    Vector.range(0, regressionDataTable.length).map { row =>
      val regressionRowMap = regressionDataTable.rowToMap(row).map { case (k, v) => k -> v.get }
      val classificationRowMap = classificationDataTable.rowToMap(row).map { case (k, v) => k -> v.get }

      require(classificationRowMap("DatasetId") == regressionRowMap("DatasetId"))
      require(classificationRowMap("ProtocolId") == regressionRowMap("ProtocolId"))
      require(classificationRowMap("GroupValues") == regressionRowMap("GroupValues"))
      require(classificationRowMap("LabelName") == regressionRowMap("LabelName"))
      require(classificationRowMap("Size") == regressionRowMap("Size"))
      require(classificationRowMap("regressionFile") == regressionRowMap("RegressionFile"))
      require(classificationRowMap("protocolFile") == regressionRowMap("ProtocolFile"))

      val tp = classificationRowMap("tp").asInstanceOf[Int]
      val fn = classificationRowMap("fn").asInstanceOf[Int]
      val fp = classificationRowMap("fp").asInstanceOf[Int]
      val tn = classificationRowMap("tn").asInstanceOf[Int]
      val stats = new ClassificationStats(tp, tn, fp, fn)
      val cost = stats.scaledCost()

      new DatasetSummary(
        classificationRowMap("DatasetId").asInstanceOf[Int], classificationRowMap("DatasetName").toString,
        classificationRowMap("ProtocolId").asInstanceOf[Int],
        classificationRowMap("ProtocolName").toString, classificationRowMap("LabelName").toString,
        classificationRowMap("GroupValues").toString, classificationRowMap("Size").asInstanceOf[Int],
        classificationRowMap("Estimator").toString, classificationRowMap("ROC_AUC").asInstanceOf[Double],
        classificationRowMap("ClassificationPointType").toString, classificationRowMap("Direction").toString,
        classificationRowMap("Threshold").asInstanceOf[Double], classificationRowMap("Accuracy").asInstanceOf[Double],
        classificationRowMap("Precision").asInstanceOf[Double], classificationRowMap("Recall").asInstanceOf[Double],
        classificationRowMap("f1").asInstanceOf[Double], classificationRowMap("Percent").asInstanceOf[Double],
        tp, fp,
        classificationRowMap("tn").asInstanceOf[Int], fn,
        classificationRowMap("correlation").asInstanceOf[Double], classificationRowMap("spearman").asInstanceOf[Double],
        classificationRowMap("regressionFile").toString, classificationRowMap("protocolFile").toString,
        cost, row,
        regressionRowMap("Estimator").toString,
        regressionRowMap("Correlation").asInstanceOf[Double], regressionRowMap("Spearman").asInstanceOf[Double]
      )
    }.sortBy(_.name())
  }

  def dataTableColumn[A](title: String, func: DatasetSummary => A): DataTableColumn[A] = {
    val values = summaries.map { s => func(s) }
    DataTableColumn.fromVector(title, values)
  }

  def summariesToDataTable(): DataTable = {
    val columns = Vector(
      dataTableColumn("DatasetId", _.datasetId),
      dataTableColumn("ProtocolId", _.protocolId),
      dataTableColumn("ProtocolName", _.protocolName),
      dataTableColumn("LabelName", _.labelName),
      dataTableColumn("GroupValues", _.groupValues),
      dataTableColumn("Correlation", _.regressionCorrelation),
      dataTableColumn("Spearman", _.regressionSpearman),
      dataTableColumn("RocAUC", _.rocAuc),
      dataTableColumn("Accuracy", _.accuracy),
      dataTableColumn("Precision", _.precision),
      dataTableColumn("Recall", _.recall),
      dataTableColumn("Cost", _.cost),
      dataTableColumn("Kurtosis", _.statistics.getKurtosis),
      dataTableColumn("Skew", _.statistics.getSkewness),
      dataTableColumn("Category", getCategoryForDataset(_).toString)
    )
    new DataTable(columns)
  }

  def saveSummariesWithCategories(): Unit = {
    val categoryDataTable = summariesToDataTable()
    val fileName = summaryFileWithCategories.getAbsolutePath
    categoryDataTable.exportToFile(fileName, writeTypes = false)
    logger.info(s"Saved to file $fileName")
  }

  def resultsForEstimator(results: Vector[RegressorValidation], estimator: String): RegressorValidation = {
    val r = results.find { r => r.estimator.getEstimatorName() == estimator }
    require(r.isDefined)
    r.get
  }

  def statsFromResults(regressorResults: RegressorValidation): DescriptiveStatistics = {
    val regressionDt = regressorResults.dataSplit.getPredictDt()
    val inputValues = regressionDt.column("label").toDoubles()
    new DescriptiveStatistics(inputValues.toArray)
  }
}

class DatasetSummary(val datasetId: Int, val datasetName: String, val protocolId: Int,
                     val protocolName: String, val labelName: String,
                     val groupValues: String, val size: Int,
                     val classificationEstimator: String, val rocAuc: Double,
                     val classificationPointType: String, val direction: String,
                     val threshold: Double, val accuracy: Double,
                     val precision: Double, val recall: Double,
                     val f1: Double, val percent: Double,
                     val tp: Int, val fp: Int,
                     val tn: Int, val fn: Int,
                     val classificationCorrelation: Double, val classificationSpearman: Double,
                     val regressionFile: String, val protocolFile: String,
                     val cost: Double, val row: Int,
                     val regressionEstimator: String,
                     val regressionCorrelation: Double, val regressionSpearman: Double
                    ) extends HasLogging with SummaryItem {
  lazy val statistics: DescriptiveStatistics = toStatistics()
  lazy val datasetCategoryKey: DatasetCategoryKey = toDatasetCategoryInformation()

  def regressionResults(): Vector[RegressorValidation] = {
    require(new File(regressionFile).exists())
    val (_, regressionResults) = Util.deserializeObject[(RegressionTable, Vector[RegressorValidation])](regressionFile)
    regressionResults
  }

  def vaultDataTableFromFields(): VaultDataTable = {
    val testLabel = if (labelName == "NA") "" else labelName
    val matchingVdts = vaultDataTables().filter(vdt =>
      vdt.labelReadout == testLabel && vdt.groupNamesString == groupValues
    )
    require(matchingVdts.length == 1)
    matchingVdts(0)
  }

  def vaultDataTables(): Vector[VaultDataTable] = {
    import DatasetSummary.dataTableCache

    val dir = new File(regressionFile).getParentFile
    val vaultDataFile = new File(dir, "vaultDataTables.obj")
    require(vaultDataFile.exists())
    val fileName = vaultDataFile.getAbsolutePath
    dataTableCache.getOrElseUpdate(fileName, Util.deserializeObject[Vector[VaultDataTable]](fileName))
  }

  def vaultDataTable(): VaultDataTable = {
    val regressionFileBase = new File(regressionFile).getName
    val datatableOpt = vaultDataTables().find { vdt =>
      val vaultPrefix = vdt.informationMd5()
      regressionFileBase == s"regression_${vaultPrefix}.obj"
    }
    require(datatableOpt.isDefined)
    datatableOpt.get
  }

  private def toStatistics(): DescriptiveStatistics = {
    val allRegressionResults = regressionResults()
    val regressorResults = DatasetSummary.resultsForEstimator(allRegressionResults, regressionEstimator)
    DatasetSummary.statsFromResults(regressorResults)
  }

  private def toDatasetCategoryInformation(): DatasetCategoryKey = {
    val datasetInformation = VaultReadouts.datasetInformationForProtocolFile(regressionFile)
    require(datasetInformation.isDefined)
    //val vdt = vaultDataTable()
    val vdt = vaultDataTableFromFields()
    DatasetCategoryKey(datasetInformation = datasetInformation.get,
      labelReadout = vdt.labelReadout,
      groupReadoutNamesAndValues = vdt.groupReadoutNamesAndValues,
      commonTerms = vdt.commonTerms)
  }

  def name(): String = s"$protocolName|$labelName|$groupValues"

  def category(): DatasetCategory = DatasetSummary.getCategoryForDataset(this)
}

trait SummaryItem {
  def name(): String

  def category(): DatasetCategory
}

trait SummarySceneBase extends HasLogging {
  val summaries: Vector[SummaryItem]
  val nCols: Int = 2
  val abbrevLength: Int = 100


  protected def drawTab(category: DatasetCategory): Option[Tab] = {
    val categorySummaries =
      summaries
        .filter { summary =>
          val datasetCategory = summary.category()
          datasetCategory == category
        }
    if (categorySummaries.isEmpty)
      return None
    val grid = new GridPane {
      vgap = 1.0
      hgap = 1.0
      padding = Insets(0.0)
      categorySummaries
        .zipWithIndex
        .foreach { case (summary, no) =>
          val row = no / nCols
          val col = no % nCols
          val name = summary.name
          val button = new Button(StringUtils.abbreviateMiddle(name, "..", abbrevLength)) {
            onAction = (e: ActionEvent) => {
              showSummary(summary)
            }
          }
          add(button, col, row)
          logger.info(s"Added button for $name")
        }
      logger.info("added all buttons")
    }

    val tab = new Tab() {
      text = category.toString
      content = grid
    }
    Some(tab)
  }

  protected def showSummary(item: SummaryItem): Unit
}

class SummaryScene(val summaries: Vector[SummaryItem]) extends Scene(800, 600) with HasLogging with SummarySceneBase {

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

    val saveButton = new Button("Save CSV") {
      onAction = (e: ActionEvent) => {
        DatasetSummary.saveSummariesWithCategories()
      }
    }
    val buttonPane = new HBox()
    buttonPane.children.add(saveButton)
    val contentPane = new VBox()
    contentPane.children.add(buttonPane)
    contentPane.children.add(tabbedPane)

    val scrollPane = new ScrollPane with SceneScrollPane {
      bindSize(SummaryScene.this)
    }
    content = scrollPane
    scrollPane.content = contentPane
  }

  override def showSummary(item: SummaryItem): Unit = {
    val summary = item.asInstanceOf[DatasetSummary]
    val scene = new DatasetSummaryScene(this)
    scene.drawScrollPane(summary)
    val stage = new Stage {
      title = s"${summary.protocolName} information"
    }
    stage.scene = scene
    stage.show()
  }

}

class DatasetSummaryScene(summaryScene: SummaryScene) extends Scene(800, 600) with HasLogging with HasSwingCharts {

  def drawScrollPane(summary: DatasetSummary) = {
    val scenePane = drawScene(summary)
    val scrollPane = new ScrollPane with SceneScrollPane {
      bindSize(DatasetSummaryScene.this)
    }
    content = scrollPane
    scrollPane.content = scenePane
  }

  private def printPair[A](g: GridPane, y: Int, name: String, value: A, x: Int = 0, xSpan: Int = 1) = {
    g.add(new Label(name), x, y)
    g.add(new Label(value.toString), x + 1, y, xSpan, 1)
    y + 1
  }

  private def drawScene(summary: DatasetSummary): Pane = {
    new GridPane {
      vgap = 5.0
      hgap = 5.0
      padding = Insets(5.0)

      var y = 0
      y = printPair(this, y, "Protocol Name", summary.protocolName, xSpan = 8)
      y = printPair(this, y, "Label Name", summary.labelName, xSpan = 8)
      y = printPair(this, y, "Group Values", summary.groupValues, xSpan = 8)
      y = printPair(this, y, "Classification Estimator", summary.classificationEstimator, xSpan = 8)
      printPair(this, y, "correlation", summary.classificationCorrelation)
      y = printPair(this, y, "spearman", summary.classificationSpearman, 2)
      y = printPair(this, y, "Size", summary.size)
      printPair(this, y, "# positive", summary.tp + summary.fn)
      y = printPair(this, y, "# negative", summary.fp + summary.tn, 2)
      printPair(this, y, "tp", summary.tp)
      y = printPair(this, y, "fp", summary.fp, 2)
      printPair(this, y, "tn", summary.tn)
      y = printPair(this, y, "fn", summary.fn, 2)
      printPair(this, y, "precision", summary.precision)
      y = printPair(this, y, "recall", summary.recall, 2)
      printPair(this, y, "f1", summary.f1, 2)
      y = printPair(this, y, "cost", summary.cost)
      printPair(this, y, "percent", summary.percent)
      printPair(this, y, "threshold", summary.threshold, 2)
      y = printPair(this, y, "direction", summary.direction, 4)
      printPair(this, y, "rocAuc", summary.rocAuc)
      y = printPair(this, y, "accuracy", summary.accuracy, 2)
      y = printPair(this, y, "Regression Estimator", summary.regressionEstimator, xSpan = 8)
      printPair(this, y, "correlation", summary.regressionCorrelation)
      y = printPair(this, y, "spearman", summary.regressionSpearman, 2)

      val allRegressionResults = summary.regressionResults()
      val regressorResults = DatasetSummary.resultsForEstimator(allRegressionResults, summary.regressionEstimator)
      assert(regressorResults.correlation == summary.regressionCorrelation)
      val regressionDt = regressorResults.dataSplit.getPredictDt()
      val stats = DatasetSummary.statsFromResults(regressorResults)

      printPair(this, y, "mean", stats.getMean)
      printPair(this, y, "median", stats.getPercentile(50.0), 2)
      y = printPair(this, y, "geom mean", stats.getGeometricMean, 4)
      printPair(this, y, "skewness", stats.getSkewness)
      printPair(this, y, "kurtosis", stats.getKurtosis, 2)
      y = printPair(this, y, "Std dev", stats.getStandardDeviation, 4)

      val categoryNames = DatasetCategory.values.map(_.toString).toSeq
      val categoryBox = new ComboBox[String](categoryNames) {
        onAction = (e: ActionEvent) => {
          val category = value.get()
          DatasetSummary.setCategoryForDataset(summary,
            DatasetCategory.withName(category))
          summaryScene.redraw()
        }
      }
      val currentCategory =
        DatasetSummary.getCategoryForDataset(summary)
      categoryBox.setValue(currentCategory.toString)

      add(new Label("Category"), 0, y)
      add(categoryBox, 1, y, 2, 1)
      y += 1

      val hbox = new HBox() {
        padding = Insets(10)
      }

      val regressionEvaluator = new RegressionEvaluator().setMetric("rmse")
      val regressionChart = regressionEvaluator.chart(regressionDt, "Regression", width = 400, height = 400, fontSize = 10, showLine = false)
      hbox.children.add(chartNode(regressionChart))
      //add(chartNode(regressionChart), 0, y, 6, 1)

      val regressorClassifierResults = DatasetSummary.resultsForEstimator(allRegressionResults, summary.classificationEstimator)
      assert(regressorClassifierResults.correlation == summary.classificationCorrelation)
      val regressionClassifierDt = regressorClassifierResults.dataSplit.getPredictDt()
      val direction = AssayDirection.fromString(summary.direction)
      val greaterBetter = direction == AssayDirection.UP || direction == AssayDirection.UP_EQUAL
      val classificationDtOption = new RegressionToClassification().toClassification(regressionClassifierDt, summary.threshold, greaterBetter)
      require(classificationDtOption.isDefined)
      val classificationDt = classificationDtOption.get
      val classificationEvaluator = new ClassificationEvaluator().setMetric("rocAuc")
      val classificationChart = classificationEvaluator.chart(classificationDt, width = 400, height = 400, fontSize = 10)
      hbox.children.add(chartNode(classificationChart))
      //add(chartNode(classificationChart), 6, y, 8, 1)

      val vaultDataTable = summary.vaultDataTable()
      val histogram = vaultDataTable.toHistogram()
      val histogramChart = histogram.plot(400, 400, legend = "Histogram", fontSize = 10)
      hbox.children.add(chartNode(histogramChart))

      add(hbox, 0, y, 8, 1)
      y += 1

      val hbox2 = new HBox() {
        padding = Insets(10)
      }

      val summaries = summaryScene.summaries.map(_.asInstanceOf[DatasetSummary])
      val nextOpt = summaries.find(s => s.row > summary.row)
      if (nextOpt.isDefined) {
        val nextButton = new Button("Next") {
          onAction = (e: ActionEvent) => {
            val nextSummary = nextOpt.get
            drawScrollPane(nextSummary)
          }
        }
        hbox2.children.add(nextButton)
      }

      val prevOpt = summaries.reverseIterator.find(s => s.row < summary.row)
      if (prevOpt.isDefined) {
        val nextButton = new Button("Previous") {
          onAction = (e: ActionEvent) => {
            val prevSummary = prevOpt.get
            drawScrollPane(prevSummary)
          }
        }
        hbox2.children.add(nextButton)
      }

      val nextUnclassifiedOpt = summaries
        .find(s => s.row > summary.row &&
          DatasetSummary.getCategoryForDataset(s) == DatasetCategory.Unassigned)
      if (nextUnclassifiedOpt.isDefined) {
        val nextButton = new Button("Next unassigned") {
          onAction = (e: ActionEvent) => {
            val nextSummary = nextUnclassifiedOpt.get
            drawScrollPane(nextSummary)
          }
        }
        hbox2.children.add(nextButton)
      }

      if (currentCategory != DatasetCategory.Unassigned) {
        val nextSameOpt = summaries
          .find(s => s.row > summary.row &&
            DatasetSummary.getCategoryForDataset(s) == currentCategory)
        if (nextSameOpt.isDefined) {
          val nextButton = new Button(s"Next $currentCategory") {
            onAction = (e: ActionEvent) => {
              val nextSummary = nextSameOpt.get
              drawScrollPane(nextSummary)
            }
          }
          hbox2.children.add(nextButton)
        }
      }
      add(hbox2, 0, y, 8, 1)
    }
  }


}

