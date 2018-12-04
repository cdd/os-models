package com.cdd.models.tox.tuning

import java.io.{File, FileWriter}

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.estimator.ClassifierProbabilityToPredictionOptimizer
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.tox._
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter

object ParameterizedModel extends HasLogging {

  class Result(val testRocAuc: Double, val testBa: Double, val scoreRocAuc: Double, val scoreBa: Double)

  def processResultsFile(toxField: String, estimatorName: String): (Double, Pipeline) = {
    val resultsFile = Tox21Tune.resultsFile(toxField, estimatorName)
    val dataTable = DataTable.loadFile(resultsFile.getAbsolutePath)

    val best = dataTable.rows().map { row =>
      row.mapValues(_.get)
    }.maxBy { row =>
      require(row("NAME").asInstanceOf[String] == toxField)
      require(row("METHOD").asInstanceOf[String] == estimatorName)
      row("rocAuc").asInstanceOf[Double]
    }

    val bestRocAuc = best("rocAuc").asInstanceOf[Double]
    val parameters = best.filterKeys {
      case "NAME" => false
      case "METHOD" => false
      case k if k.startsWith("rocAuc") => false
      case _ => true
    }

    val estimators = BestClassificationEstimators.estimators.filter(_.getEstimatorName() == estimatorName)
    require(estimators.length == 1)
    // need to copy as BestClassificationEstimators.estimators is shared
    val estimator = estimators(0).copy(new ParameterMap())

    for ((parameterName, parameterValue) <- parameters) {
      val stageOpt = estimator.getParameter(estimator.stages).find(s => s.hasParameter(parameterName))
      require(stageOpt.isDefined)
      //val stage = stageOpt.get.copy(new ParameterMap())
      val stage = stageOpt.get
      val parameter = stage.getParameterByName(parameterName)
      val oldValue = stage.getParameter(parameter)
      stage.setParameter(parameter, parameterValue)
      logger.info(s"Parameter $parameterName in stage ${stage.uid} $oldValue -> $parameterValue")
    }
    val abOptimizer = new ClassifierProbabilityToPredictionOptimizer().setOptimizerFunction(Tox21ClassificationModel.optimizerFunction)
    estimator.appendStage(abOptimizer)

    (bestRocAuc, estimator)
  }
}

  trait ParameterizedModelAppTrait {

  def modelObjectFile(toxField: String, estimatorName: String): File

  def savedModelFile(toxField: String): File

  def bestModel(toxField: String): PipelineModel

  def inputTable(toxField: String): DataTable

  val resultsFile:File
  val summaryFile:File

  def build(): Unit = {

    Util.using(new FileWriter(resultsFile)) { out =>
      val csvWriter = new CSVWriter(out)
      csvWriter.writeNext(Tox21ClassificationModel.resultHeaders)

      Util.using(new FileWriter(summaryFile)) { out2 =>
        val summaryWriter = new CSVWriter(out2)
        summaryWriter.writeNext(Array("ToxField", "RocAUC", "BA"))

        val results = Tox21ClassificationModel.toxAssays.map { toxField =>
          val tox21Results = processToxField(toxField)
          Tox21ClassificationModel.report(tox21Results, csvWriter, summaryWriter)
          summaryWriter.flush()
          csvWriter.flush()
          tox21Results
        }

        val imageFile = Util.getProjectFilePath("data/Tox21/tox21_10k_data_models.png")
        Tox21ClassificationModel.classificationPlots(results, imageFile.getAbsolutePath, nCols = 4)
      }
    }
  }

  def processToxField(toxField: String): Tox21Results = {

    val savedDataFile = savedModelFile(toxField)
    Util.withSerializedFile(savedDataFile) { () =>
      val estimatorNames = BestClassificationEstimators.estimators.map(_.getEstimatorName())
      val (classificationResults, models) = estimatorNames.map { estimatorName =>
        val resultsFile = Tox21Tune.resultsFile(toxField, estimatorName)
        if (resultsFile.exists()) {
          val parameterizedModel = new ParameterizedModel(toxField, estimatorName, inputTable(toxField))
          val modelFile = modelObjectFile(toxField, estimatorName)
          (parameterizedModel.crossValidate(), parameterizedModel.tox21Model(modelFile))
        } else {
          throw new RuntimeException(s"Missing results for assay $toxField estimator $estimatorName")
        }
      }.sortBy { case (v, _) => -v.metric }
        .unzip

      new Tox21Results(toxField, classificationResults, models)
    }
  }
}

class ParameterizedModel(val toxField: String, val estimatorName: String, val inputTable: DataTable) extends HasLogging {

  import ParameterizedModel._

  val (bestRocAuc, estimator) = processResultsFile(toxField, estimatorName)

  // This validation routine should only be use for when the scoring dataset is not included in the training dataset
  def validate(): Unit = {
    val evaluator = new ClassificationEvaluator()
    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(estimator).setNumFolds(3)
    dataSplit.foldAndFit(inputTable)
    val predictTable = dataSplit.getPredictDt()
    val rocAuc = evaluator.rocAuc(predictTable)
    val stats = evaluator.statistics(predictTable)
    // the ROC AUC we got in hyperparameter optimization, may not match the ROC AUC here because of the influence of random numbers
    logger.info(s"Validate: optimization ROC AUC $bestRocAuc validation ROC AUC $rocAuc BA ${stats.ba} f1 ${stats.f1}")
    if (Math.abs(rocAuc - bestRocAuc) > 0.05) {
      throw new RuntimeException(s"Excessive difference in ROC AUC: optimization ROC AUC $bestRocAuc validation ROC AUC $rocAuc ")
    }
  }

  def build(modelFile:File): PipelineModel = {
    Util.withSerializedFile(modelFile) { () =>
      val model =
        estimator.fit(inputTable)
      val dataTable = model.transform(inputTable)
      val evaluator = new ClassificationEvaluator()
      val rocAuc = evaluator.rocAuc(dataTable)
      val stats = evaluator.statistics(dataTable)
      logger.info(s"Build: optimization ROC AUC $bestRocAuc validation ROC AUC $rocAuc BA ${stats.ba} f1 ${stats.f1}")
      model
    }
  }

  def crossValidate(): ClassifierValidation = {
    val evaluator = new ClassificationEvaluator().setMetric("rocAuc")
    val tester = new BestClassificationEstimators(evaluator = evaluator)
    val classificationResults = tester.fitDataTable(toxField, "tox21", inputTable, estimators = Vector(estimator))
    classificationResults(0)
  }

  def tox21Model(modelFile:File): Tox21Model = {
    val (testDf, scoreDf) = Tox21Compounds.buildTestAndScoreDf()
    val model = build(modelFile)
    val testDataTable = Tox21ClassificationModel.predict("Test", toxField, estimatorName, testDf, model)
    val scoreDataTable = Tox21ClassificationModel.predict("Score", toxField, estimatorName, scoreDf, model)
    new Tox21Model(model, testDataTable, scoreDataTable)
  }

}

object ParameterizedModelApp extends ParameterizedModelAppTrait with HasLogging {

  override def modelObjectFile(toxField: String, estimatorName: String): File =
    Util.getProjectFilePath(s"data/Tox21/tuning/model_${toxField}_${estimatorName}.obj".replace(' ', '_'))

  override def savedModelFile(toxField: String): File =
    Util.getProjectFilePath(s"data/Tox21/tuning/tox21_10k_data_${toxField}_results.obj")

  override def bestModel(toxField: String): PipelineModel = {
    val results = Util.deserializeObject[Tox21Results](savedModelFile(toxField).getAbsolutePath)
    results.models(0).model
  }

  override val resultsFile:File = Util.getProjectFilePath(s"data/Tox21/tuning/tox21_10k_data_tuning_results.csv")
  override val summaryFile:File = Util.getProjectFilePath(s"data/Tox21/tuning/tox21_10k_data_tuning_summary.csv")

  override def inputTable(toxField:String):DataTable = Tox21ClassificationModel.inputTable(toxField)

  def main(args: Array[String]): Unit = {
     build()
   }
}

object ParameterizedModelAppIncludingScoring extends ParameterizedModelAppTrait with HasLogging {

  override def modelObjectFile(toxField: String, estimatorName: String): File =
    Util.getProjectFilePath(s"data/Tox21/tuning_including_scoring/model_${toxField}_${estimatorName}_with_scoring.obj".replace(' ', '_'))

  override def savedModelFile(toxField: String): File =
    Util.getProjectFilePath(s"data/Tox21/tuning_including_scoring/tox21_10k_data_${toxField}_results_with_scoring.obj")

  override def bestModel(toxField: String): PipelineModel = {
    val results = Util.deserializeObject[Tox21Results](savedModelFile(toxField).getAbsolutePath)
    results.models(0).model
  }

  override val resultsFile:File = Util.getProjectFilePath(s"data/Tox21/tuning_including_scoring/tox21_10k_data_tuning_results_with_scoring.csv")
  override val summaryFile:File = Util.getProjectFilePath(s"data/Tox21/tuning_including_scoring/tox21_10k_data_tuning_summary_with_scoring.csv")

  override def inputTable(toxField:String):DataTable = Tox21ClassificationModelIncludingScoring.inputTable(toxField)

   def main(args: Array[String]): Unit = {
     build()
   }
}