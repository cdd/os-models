package com.cdd.models.vault

import java.io.FileWriter

import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter

object PublicVaultRegressionToMultipleClassificationCutoff extends ProtocolFilesFinders with HasLogging {

  val defaultClassificationDetailFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/regression_to_classification_cutoffs_detail.csv")
  val defaultClassificationSummaryFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/regression_to_classification_cutoffs.csv")

  def main(args: Array[String]): Unit = {
    val classificationDetailFile = if (args.length > 0) args(0) else defaultClassificationDetailFile.getAbsolutePath
    val classificationSummaryFile = if (args.length > 1) args(1) else defaultClassificationSummaryFile.getAbsolutePath

    val files = if (args.length > 2) findProtocolObjects(args.drop(2)) else findProtocolObjects()

    Util.using(new FileWriter((classificationDetailFile))) { classificationDetailFh =>
      val classificationDetailWriter = new CSVWriter(classificationDetailFh)
      classificationDetailWriter.writeNext(PublicVaultEstimator.regressionToMultipleClassificationHeaders)

      Util.using(new FileWriter(classificationSummaryFile)) { classificationFh =>
        val classificationSummaryWriter = new CSVWriter(classificationFh)
        classificationSummaryWriter.writeNext(PublicVaultEstimator.regressionToMultipleClassificationHeaders)

        for (file <- files) {
          runOnProtocol(file, Some(classificationDetailWriter), Some(classificationSummaryWriter))
        }
      }
    }

    def runOnProtocol(protocolFileName: String, classificationDetailWriter: Option[CSVWriter], classificationSummaryWriter: Option[CSVWriter]): Unit = {

      val (protocolResultsDir, datasetInformation, vaultDataTables) = datasetInformationAndDataTables(protocolFileName)

      val detailWriters = classificationDetailWriter match {
        case Some(sw) => Vector(sw)
        case None => Vector.empty
      }

      val summaryWriters = classificationSummaryWriter match {
        case Some(sw) => Vector(sw)
        case None => Vector.empty
      }

      vaultDataTables.foreach { vdt =>

        val vaultEstimator = new PublicVaultEstimator(datasetInformation, vdt)
        if (vaultEstimator.regressionResultsFile.exists()) {
          vaultEstimator.regressionToMultipleClassifiers(summaryWriters, detailWriters, protocolFileName)
        } else {
          logger.warn("No regression results for protocol $protocolName label $labelName group names and values $groupValues")
        }

      }
    }
  }
}
