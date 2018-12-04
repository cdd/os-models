package com.cdd.models.tox

import com.cdd.models.datatable.{DataTableColumn, DataTableModelUtil}
import com.cdd.models.pipeline.{ClassifierValidation, EstimatorValidation, NamedEstimatorValidation, RegressorValidation}
import com.cdd.models.universalmetric.{DiscreteDataHistogram, DistributionType}
import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.models.vault._


case class ToxDataset(datasetName: String, protocolName: String, readoutNames: Vector[String]) extends HasLogging {

  import VaultToxDatasets._

  logger.info(s"datasetName $datasetName protocolName $protocolName readoutNames ${readoutNames.mkString("|")}")

  val dataset = VaultDownload.downloadDatasets(vaultId).find(_.name == datasetName).get
  val protocol = VaultDownload.downloadProtocolsForDataset(vaultId, dataset.id).find(_.name == protocolName).get

  def process(): Vector[NamedEstimatorValidation] = {
    readoutNames.flatMap(process(_).map(rs => rs(0)))
  }

  def process(readoutName: String): Vector[Vector[NamedEstimatorValidation]] = {
    logger.info(s"Processing dataset $datasetName protocolName $protocolName readoutName $readoutName")

    val downloader = new DownloadFromVault(vaultId, Some(dataset.id), readoutName, Some(protocol.id))
    val dt = downloader.downloadFile()

    val name = s"${protocol.name}_$readoutName"
    val histogram = new DiscreteDataHistogram(dt.column("label").asInstanceOf[DataTableColumn[Double]].values)
    if (histogram.isClassification()) {
      // classification
      val classifierValues = histogram.binaryClassification().get
      val classificationDt = dt.rename("label" -> "rawLabel").addColumn(new DataTableColumn[Double]("label", classifierValues))
      val classification = new VaultClassificationModel(classificationDt, name)
      val results = classification.applyClassifiers()
      val namedResults = results.map {
        NamedEstimatorValidation(name, _)
      }
      VaultClassificationModel.saveClassificationResults(name, downloader.downloadDir(), results,
        fileNamePrefix = readoutName)
      Vector(namedResults)
    } else {
      // regression
      val regression = new VaultRegressionModel(dt, name)
      val results = regression.applyRegressors()
      val namedResults = results.map {
        NamedEstimatorValidation(name, _)
      }
      VaultRegressionModel.saveRegressionResults(name, downloader.downloadDir(), results,
        fileNamePrefix = readoutName)

      regression.applyLogRegressors() match {
        case Some(logResults) =>
        val logName = s"log_${protocol.name}_$readoutName"
        val namedLogResults = logResults.map {
          NamedEstimatorValidation(logName, _)
        }
        VaultRegressionModel.saveRegressionResults(logName, downloader.downloadDir(), logResults,
          fileNamePrefix = s"log_$readoutName")
        Vector(namedResults, namedLogResults)
        case None => Vector(namedResults)
      }
    }
  }
}

object VaultToxDatasets {
  val vaultId = 1

  val toxDatasets: Vector[ToxDataset] = Vector(
    ToxDataset("MULTIPLE SCLEROSIS: OL Toxicity Screening", "Oligodendrocyte Toxicity Assay",
      Vector("Average at 10 uM", "Average at 1 uM", "Average at 0.1 uM")),

    ToxDataset("TOX: Drug induced liver injury data (DILI)", "DILI", Vector("DILI (1 = yes, 0 = no)")),

    ToxDataset("FDA APPROVED, TOX: Maximum recommended daily dose", "Maximum recommended daily dose",
      Vector("MRDD dose (mM)")),

    ToxDataset("TOX: UC Davis - Hammock's Public Data", "Human sEH", Vector("Human sEH IC50")),
    ToxDataset("TOX: UC Davis - Hammock's Public Data", "Mouse sEH", Vector("Mouse sEH IC50"))
  )

  def process() = {
    val results = toxDatasets.flatMap {
      _.process()
    }

    val regressionResults = results.filter(r => r.estimatorValidation.isInstanceOf[RegressorValidation] && !r.name.startsWith("log_"))
    val regressionImageFile = Util.getProjectFilePath("data/vault/vault_tox_regressors.png")
    VaultRegressionModel.regressionPlots(regressionResults.map(_.estimatorValidation.asInstanceOf[RegressorValidation]),
      regressionImageFile.getAbsolutePath, cols = 3, names = regressionResults.map(_.name))

    val logRegressionResults = results.filter(r => r.estimatorValidation.isInstanceOf[RegressorValidation] && r.name.startsWith("log_"))
    val logRegressionImageFile = Util.getProjectFilePath("data/vault/vault_tox_log_regressors.png")
    VaultRegressionModel.regressionPlots(logRegressionResults.map(_.estimatorValidation.asInstanceOf[RegressorValidation]),
      logRegressionImageFile.getAbsolutePath, cols = 3, names = logRegressionResults.map(_.name))

    val classificationResults = results.filter(r => r.estimatorValidation.isInstanceOf[ClassifierValidation])
    val classificationImageFile = Util.getProjectFilePath("data/vault/vault_tox_classifiers.png")
    VaultClassificationModel.classificationPlots(classificationResults.map(_.estimatorValidation.asInstanceOf[ClassifierValidation]),
      classificationImageFile.getAbsolutePath, cols = 1, names = classificationResults.map(_.name))
  }

  def main(args: Array[String]): Unit = {
    process()
  }
}
