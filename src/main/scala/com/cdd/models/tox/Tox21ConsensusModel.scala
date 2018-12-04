package com.cdd.models.tox

import java.io.{File, FileWriter}

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.ClassifierProbabilityToPredictionOptimizer
import com.cdd.models.pipeline.{ClassificationEvaluator, ConsensusProbabilityClassificationModel, PipelineModel}
import com.cdd.models.tox.Tox21ConsensusModel.ToxResultSet
import com.cdd.models.tox.Tox21ConsensusModel.ToxResultSetType.ToxResultSetType
import com.cdd.models.tox.tuning.{ParameterizedModelApp, ParameterizedModelAppIncludingScoring}
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter
import org.apache.commons.lang3.StringUtils


object Tox21ConsensusModel extends HasLogging {

  class ToxResultSet(val outputDir: File, val savedModelFile: (String) => File)

  object ToxResultSetType extends Enumeration {
    type ToxResultSetType = Value
    val Best, Tuned, BestIncludingScoring, TunedIncludingScoring = Value
  }

  def toxResultSetFromType(toxResultSetType: ToxResultSetType): ToxResultSet = {
    toxResultSetType match {
      case ToxResultSetType.Best =>
        new ToxResultSet(Util.getProjectFilePath("data/Tox21/consensus"), Tox21ClassificationModel.savedModelFile)
      case ToxResultSetType.Tuned =>
        new ToxResultSet(Util.getProjectFilePath("data/Tox21/consensus_tuning"), ParameterizedModelApp.savedModelFile)
      case ToxResultSetType.BestIncludingScoring =>
        new ToxResultSet(Util.getProjectFilePath("data/Tox21/consensus_including_scoring"),
          Tox21ClassificationModelIncludingScoring.savedModelFile)
      case ToxResultSetType.TunedIncludingScoring =>
        new ToxResultSet(Util.getProjectFilePath("data/Tox21/consensus_tuning_including_scoring"),
          ParameterizedModelAppIncludingScoring.savedModelFile)
      case _ => throw new IllegalArgumentException
    }
  }

  def applyConsensusModels(toxResultSet: ToxResultSet): Unit = {
    val resultFile = new File(toxResultSet.outputDir, "tox21_10k_data_consensus_results.csv")
    // first build all the models
    Tox21ClassificationModel.toxAssays.foreach { toxField =>
      val consensusModel = new Tox21ConsensusModel(toxResultSet, toxField)
      consensusModel.buildModel()
    }

    Util.using(new FileWriter(resultFile)) { out2 =>
      val resultWriter = new CSVWriter(out2)
      resultWriter.writeNext(Tox21ClassificationModel.resultHeaders)

      val dataTables = Tox21ClassificationModel.toxAssays.map { toxField =>
        logger.info(s"Processing tox field $toxField")
        val consensusModel = new Tox21ConsensusModel(toxResultSet, toxField)
        val dataTable = consensusModel.testModel(resultWriter)
        resultWriter.flush()
        dataTable
      }
      consensusClassificationPlots(toxResultSet, dataTables)
    }
  }

  private def consensusClassificationPlots(toxResultSet: ToxResultSet, dataTables: Vector[DataTable]) = {
    val evaluator = new ClassificationEvaluator()

    val charts = dataTables.zip(Tox21ClassificationModel.toxAssays).map { case (predictTable, toxField) =>
      val title = StringUtils.abbreviate(s"$toxField".trim, 60)
      evaluator.chart(predictTable, title)
    }
    val imageFile = new File(toxResultSet.outputDir, "tox21_10k_data_consensus_models.png")
    Util.gridPlot(imageFile.getAbsolutePath, charts, 4)
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val toxResultSet = toxResultSetFromType(ToxResultSetType.withName(args(0)))
    applyConsensusModels(toxResultSet)
  }

  def modelFile(toxResultSet: ToxResultSet, toxField: String): File =
    new File(toxResultSet.outputDir, s"consensus_model_$toxField.obj")

  // TODO cache models
  def model(toxResultSet: ToxResultSet, toxField: String): PipelineModel =
    Util.deserializeObject[PipelineModel](modelFile(toxResultSet, toxField).getAbsolutePath)

}

class Tox21ConsensusModel(toxResultSet: ToxResultSet, toxField: String) {

  def buildModel(): PipelineModel = {
    Util.withSerializedFile(Tox21ConsensusModel.modelFile(toxResultSet, toxField)) { () =>
      val saveFile = toxResultSet.savedModelFile(toxField)
      require(saveFile.exists())
      val tox21Results = Util.deserializeObject[Tox21Results](saveFile.getAbsolutePath)
      assert(tox21Results.toxField == toxField)

      // drop last model to exclude Bayesian classifier
      val (models, weights) = tox21Results.models.zip(tox21Results.results).map { case (m, r) =>
        val model = m.model
        val metric = r.metric
        (model, metric)
      }.dropRight(1).unzip

      val consensusModel = new ConsensusProbabilityClassificationModel(models, weights)
      val inputTable = Tox21ClassificationModel.inputTable(toxField)
      val consensusDataTable = consensusModel.transform(inputTable)

      // add in optimizer from probability:
      val optimizer = new ClassifierProbabilityToPredictionOptimizer().setOptimizerFunction(Tox21ClassificationModel.optimizerFunction)
      val optimizerModel =optimizer.fit(consensusDataTable)
      val pipeline = new PipelineModel(Vector(consensusModel, optimizerModel))
      pipeline
    }
  }

  def testModel(csvWriter: CSVWriter): DataTable = {
    val consensusModel = Tox21ConsensusModel.model(toxResultSet, toxField)
    val (testDf, scoreDf) = Tox21Compounds.buildTestAndScoreDf()
    val testDataTable = Tox21ClassificationModel.predict("Test", toxField, "consensus", testDf, consensusModel)
    val scoreDataTable = Tox21ClassificationModel.predict("Score", toxField, "consensus", scoreDf, consensusModel)

    val inputTable = Tox21ClassificationModel.inputTable(toxField)
    val consensusDataTableWithOptimization = consensusModel.transform(inputTable)
    Tox21ClassificationModel.writeResult(csvWriter, toxField, "Training", "Consensus", consensusDataTableWithOptimization)
    Tox21ClassificationModel.writeResult(csvWriter, toxField, "Test", "Consensus", testDataTable)
    Tox21ClassificationModel.writeResult(csvWriter, toxField, "Score", "Consensus", scoreDataTable)

    scoreDataTable
  }

}
