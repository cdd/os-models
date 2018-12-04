package com.cdd.models.tox

import java.io.{File, FileWriter}

import com.cdd.models.datatable.{DataTable, DataTableModelUtil}
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.estimator.ClassifierProbabilityToPredictionOptimizer
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter
import org.apache.commons.lang3.StringUtils

@SerialVersionUID(1000L)
class Tox21Model(val model: PipelineModel, val testPrediction: DataTable, val scorePrediction: DataTable) extends Serializable

@SerialVersionUID(1000L)
class Tox21Results(val toxField: String, val results: Vector[ClassifierValidation], val models: Vector[Tox21Model]) extends Serializable

trait Tox21ModelBuilder extends HasLogging {
  val optimizerFunction: String = "f1" //"ba"
  val toxAssays: Vector[String] = Vector("NR-AR", "NR-AhR", "NR-AR-LBD", "NR-ER", "NR-ER-LBD", "NR-Aromatase", "NR-PPAR-gamma",
    "SR-ARE", "SR-ATAD5", "SR-HSE", "SR-MMP", "SR-p53")
  val selectColumns: Vector[String] =
    Vector("no", "cdk_smiles", "fingerprints_CDK_ECFP6", "fingerprints_CDK_FCFP6", "cdk_descriptors",
      "rdkit_smiles", "fingerprints_RDKit_ECFP6", "fingerprints_RDKit_FCFP6", "rdkit_descriptors") ++ toxAssays
  val resultHeaders: Array[String] = Array("ToxAssay", "Method", "Dataset", "TP", "FP", "TN", "FN", "RocAUC", "BA", "f1", "Precision", "Recall", "Specificity", "Count")
  private val includeLeaderInTraining = true

  def savedModelFile(toxField: String): File

  def estimators(): Vector[Pipeline]

  def resultsFile: File

  def summaryFile: File

  def imageFile: File

  def inputTable(toxField:String): DataTable

  def trainAndCrossValidate(toxField: String): Tox21Results = {
    val savedDataFile = savedModelFile(toxField)
    Util.withSerializedFile(savedDataFile) { () =>
      val evaluator = new ClassificationEvaluator().setMetric("rocAuc")
      val inputTable = this.inputTable(toxField)
      val allToxTable = this.allToxTable(toxField)
      val bestEstimators = BestClassificationEstimators.estimators
      val abOptimizer = new ClassifierProbabilityToPredictionOptimizer().setOptimizerFunction(optimizerFunction)
      bestEstimators.foreach { e => e.appendStage(abOptimizer) }
      val tester = new BestClassificationEstimators(evaluator = evaluator)
      val classificationResults = tester.fitDataTable(toxField, "tox21", inputTable, estimators = this.estimators())

      val (testDf, scoreDf) = Tox21Compounds.buildTestAndScoreDf()

      val models = classificationResults.zipWithIndex.map { case (result, index) =>
        val dt = result.dataSplit.getPredictDt()
        val estimatorName = result.estimator.getEstimatorName()
        val model = result.estimator.fit(inputTable).asInstanceOf[PipelineModel]
        val testDataTable = predict("Test", toxField, estimatorName, testDf, model)
        val scoreDataTable = predict("Score", toxField, estimatorName, scoreDf, model)
        new Tox21Model(model, testDataTable, scoreDataTable)
      }

      new Tox21Results(toxField, classificationResults, models)
    }
  }

  def predict(name: String, toxField: String, estimatorName: String,
              testDf: DataTable, model: PipelineModel): DataTable = {
    val dataTable = DataTableModelUtil.selectTable(testDf, toxField)
    val predictTable = model.transform(dataTable)
    predictTable
  }

  def challengeTable(toxField: String): DataTable = {
    val baseDf = Tox21Compounds.buildDf()
    val dataTable = if (includeLeaderInTraining) {
      val (testDf, _) = Tox21Compounds.buildTestAndScoreDf()
      baseDf.append(testDf)
    } else {
      baseDf
    }
    DataTableModelUtil.selectTable(dataTable, toxField)
  }

  def allToxTable(toxField: String): DataTable = {
    val baseDf = Tox21Compounds.buildDf()
    val (testDf, scoreDf) = Tox21Compounds.buildTestAndScoreDf()
    val dataTable = baseDf.append(testDf).append(scoreDf)
    DataTableModelUtil.selectTable(dataTable, toxField)
  }

  def writeResult(csvWriter: CSVWriter, toxField: String, dataset: String, estimatorName: String,
                  dataTable: DataTable): (Double, Double) = {
    val evaluator = new ClassificationEvaluator()
    val stats = evaluator.statistics(dataTable)
    val rocAuc = evaluator.rocAuc(dataTable)
    val ba = stats.ba
    csvWriter.writeNext(Array(
      toxField,
      estimatorName,
      dataset,
      stats.tp.toString,
      stats.fp.toString,
      stats.tn.toString,
      stats.fn.toString,
      rocAuc.toString,
      ba.toString,
      stats.f1.toString,
      stats.precision.toString,
      stats.recall.toString,
      stats.specificity.toString,
      dataTable.length.toString))
    (rocAuc, ba)
  }

  def bestModel(toxField: String): PipelineModel = {
    val results = Util.deserializeObject[Tox21Results](savedModelFile(toxField).getAbsolutePath)
    results.models(0).model
  }

  def report(results: Tox21Results, csvWriter: CSVWriter, summaryWriter: CSVWriter): Unit = {
    val toxField = results.toxField

    results.models.zip(results.results).zipWithIndex.foreach { case ((model, result), index) =>
      val dt = result.dataSplit.getPredictDt()
      val estimatorName = result.estimator.getEstimatorName()

      val (rocAuc, _) = writeResult(csvWriter, toxField, "Training", result.estimator.getEstimatorName(), dt)
      assert(rocAuc == result.metric)
      val (testRocAuc, testBa) = writeResult(csvWriter, toxField, "Test", estimatorName, model.testPrediction)
      val (scoreRocAuc, scoreBa) = writeResult(csvWriter, toxField, "Score", estimatorName, model.scorePrediction)
      if (index == 0) {
        summaryWriter.writeNext(Array(toxField, scoreRocAuc.toString, scoreBa.toString))
      }
    }
  }

  def classificationPlots(tox21Results: Vector[Tox21Results], fileName: String, nCols: Int = 4): Unit = {
    val evaluator = new ClassificationEvaluator()
    val charts = tox21Results.map { result =>
      val predictTable = result.models(0).scorePrediction
      val estimatorName = result.results(0).estimator.getEstimatorName()
      val title = StringUtils.abbreviate(s"${result.toxField} $estimatorName".trim, 60)
      evaluator.chart(predictTable, title)
    }
    Util.gridPlot(fileName, charts, nCols)
  }

  def applyToxFields(): Unit = {

    Util.using(new FileWriter(resultsFile)) { out =>
      val csvWriter = new CSVWriter(out)
      csvWriter.writeNext(resultHeaders)

      Util.using(new FileWriter(summaryFile)) { out2 =>
        val summaryWriter = new CSVWriter(out2)
        summaryWriter.writeNext(Array("ToxField", "RocAUC", "BA"))

        val results = toxAssays.map { toxField =>
          logger.info(s"Processing tox field $toxField")
          val tox21Results = applyClassifiers(csvWriter, summaryWriter, toxField)
          summaryWriter.flush()
          csvWriter.flush()
          tox21Results
        }
        classificationPlots(results, imageFile.getAbsolutePath, nCols = 4)
      }
    }
  }

  private def applyClassifiers(csvWriter: CSVWriter, summaryWriter: CSVWriter, labelColumnName: String): Tox21Results = {
    val tox21Results = trainAndCrossValidate(labelColumnName)
    report(tox21Results, csvWriter, summaryWriter)
    tox21Results
  }
}

object Tox21ClassificationModel extends HasLogging with Tox21ModelBuilder {

  override def estimators(): Vector[Pipeline] = BestClassificationEstimators.estimators

  override def resultsFile: File = Util.getProjectFilePath(s"data/Tox21/best/tox21_10k_data_results.csv")

  override def summaryFile: File = Util.getProjectFilePath(s"data/Tox21/best/tox21_10k_data_summary.csv")

  override def imageFile: File = Util.getProjectFilePath("data/Tox21/best/tox21_10k_data_models.png")

  def savedModelFile(toxField: String): File =
    Util.getProjectFilePath(s"data/Tox21/best/tox21_10k_data_${toxField}_results.obj")

  override def inputTable(toxField: String): DataTable = challengeTable(toxField)

  def main(args: Array[String]): Unit = {
    applyToxFields()
  }
}


object Tox21ClassificationModelIncludingScoring extends HasLogging with Tox21ModelBuilder {

  override def estimators(): Vector[Pipeline] = BestClassificationEstimators.estimators

  override def resultsFile: File = Util.getProjectFilePath(s"data/Tox21/best_including_scoring/tox21_10k_data_results_with_scoring.csv")

  override def summaryFile: File = Util.getProjectFilePath(s"data/Tox21/best_including_scoring/tox21_10k_data_summary_with_scoring.csv")

  override def imageFile: File = Util.getProjectFilePath("data/Tox21/best_including_scoring/tox21_10k_data_models_with_scoring.png")

  def savedModelFile(toxField: String): File =
    Util.getProjectFilePath(s"data/Tox21/best_including_scoring/tox21_10k_data_${toxField}_results_with_scoring.obj")

  override def inputTable(toxField: String): DataTable = allToxTable(toxField)

  def main(args: Array[String]): Unit = {
    applyToxFields()
  }
}



