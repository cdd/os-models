package com.cdd.models.tox

import java.io.{File, FileWriter}

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.TestSmiles
import com.cdd.models.tox.PredictOnReference.StructureReferenceSet
import com.cdd.models.tox.PredictOnReference.StructureReferenceSet.StructureReferenceSet
import com.cdd.models.tox.Tox21ConsensusModel.ToxResultSetType
import com.cdd.models.tox.Tox21Tester.Tox21TesterResults
import com.cdd.models.tox.Tox21TesterModel._
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter

import scala.io.Source

object PredictOnReference extends HasLogging {

  object StructureReferenceSet extends Enumeration {
    type StructureReferenceSet = Value
    val CMC, NCI = Value
  }

  private val nciStructureFile: File = Util.getProjectFilePath("data/reference/NCI-Open_2012-05-01.smi.gz")
  private val cmcStructureFile: File = Util.getProjectFilePath("data/reference/cmc.smi.gz")
  private val nciTableFile: File = Util.getProjectFilePath("data/reference/NCI-Open_2012-05-01_data_table.csv.gz")
  private val cmcTableFile: File = Util.getProjectFilePath("data/reference/cmc_data_table.csv.gz")

  def referenceFile(ref: StructureReferenceSet): File = {
    ref match {
      case StructureReferenceSet.CMC => cmcStructureFile
      case StructureReferenceSet.NCI => nciStructureFile
    }
  }

  def referenceDataTable(ref: StructureReferenceSet): File = {
    ref match {
      case StructureReferenceSet.CMC => cmcTableFile
      case StructureReferenceSet.NCI => nciTableFile
    }
  }

  private def fileToSmiles(file: File): Vector[String] = {
    logger.info(s"Loading smiles from ${file.getAbsolutePath}")
    Util.using(Util.openFileAsStreamByName(file)) { fin =>
      val in = Source.fromInputStream(fin)
      val output = in.getLines().map { line =>
        val terms = line.split(" ")
        require(terms.length == 2)
        val (smi, _) = (terms(0), terms(1))
        smi
      }.toVector
      logger.info(s"Loaded ${output.length} smiles")
      output
    }
  }

  private def smilesToDataTable(tableFileName: File, smiles: Vector[String], parallel:Boolean): Unit = {
    logger.info("Creating data table from smiles")
    val dataTable = TestSmiles.allDataTable(smiles, parallel)
    logger.info(s"Saving data table to ${tableFileName.getAbsolutePath}")
    dataTable.exportToFile(tableFileName.getAbsolutePath)
  }

  def referenceSmiles(ref: StructureReferenceSet): Vector[String] = {
    val structureFile = referenceFile(ref)
    fileToSmiles(structureFile)
  }

  def fileToDataTable(ref: StructureReferenceSet, parallel:Boolean): Unit = {
    val tableFileName = referenceDataTable(ref)
    val smiles = referenceSmiles(ref)
    smilesToDataTable(tableFileName, smiles, parallel)
  }

  def dataTableForRef(ref: StructureReferenceSet): DataTable = {
    val tableFileName = referenceDataTable(ref)
    require(tableFileName.exists())
    DataTable.loadFile(tableFileName.getAbsolutePath)
  }

  def searchOnDataTable(ref: StructureReferenceSet, modelType: Tox21TesterModel, toxField: String): Tox21TesterResults = {
    val dataTable = dataTableForRef(ref)
    val (results, _) = Util.time {
      Tox21Tester.testDataTable(toxField, modelType, dataTable)
    }
    results
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      print("Usage: PredictOnReference <cmc|nci> < Best|Consensus|Tuned|TunedConsensus> <toxField>")
      return
    }

    val ref = StructureReferenceSet.withName(args(0).toUpperCase())
    val modelType = Tox21TesterModel.withName(args(1))
    val toxField = args(2)

    searchOnDataTable(ref, modelType, toxField)

  }
}

object PredictOnReferenceApp extends App {

  import PredictOnReference._
  val toxResultSetType = ToxResultSetType.withName(args(0))

  val resultsFile = toxResultSetType match {
    case ToxResultSetType.Best => Util.getProjectFilePath (s"data/Tox21/tox21_reference_search_results_not_tuned.csv")
    case ToxResultSetType.Tuned => Util.getProjectFilePath (s"data/Tox21/tox21_reference_search_results_tuned.csv")
  }
  val modelTypes = toxResultSetType match {
    case ToxResultSetType.Best =>  Vector(Tox21TesterModel.Best, Tox21TesterModel.Consensus)
    case ToxResultSetType.Tuned => Vector(Tox21TesterModel.TunedBest, Tox21TesterModel.TunedConsensus)
  }

  Util.using(new FileWriter(resultsFile)) { out =>
    val csvWriter = new CSVWriter(out)
    csvWriter.writeNext(Array("dataset", "model", "estimator", "features", "toxField",
      "time", "dataset_compounds", "tested_compounds", "n_predicted", "hitrate"))

    for (ref <- StructureReferenceSet.values) {
      val dataTable = dataTableForRef(ref)
      for {
        toxField <- Tox21ClassificationModel.toxAssays
        modelType <- modelTypes
      } {

        val (results, time) = Util.time {
          Tox21Tester.testDataTable(toxField, modelType, dataTable, parallel = true)
        }

        csvWriter.writeNext(Array(ref.toString, modelType.toString, results.model.getEstimatorId(),
          results.model.getFeaturesColumn(), toxField, (time.toDouble/1000.0).toString,
          dataTable.length.toString, results.predictTable.length.toString,
          results.nPredicted.toString, results.hitRate.toString))

        csvWriter.flush()
      }
    }
  }
}

object BuildDataTablesApp extends App {

  import PredictOnReference._

  Util.time {
    fileToDataTable(StructureReferenceSet.withName(args(0).toUpperCase()), false)
  }
}

