package com.cdd.models.tox

import com.cdd.models.datatable.DataTableModelUtil
import com.cdd.models.pipeline.{NamedClassifierValidation, NamedRegressorValidation}
import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.models.vault.VaultDownload.{downloadDatasets, downloadProjects, downloadProtocolsForDataset, downloadProtocolsForProject}
import com.cdd.models.vault._

class AzAmdeData {
  val vaultId = 1 // CDD Vault
  val dataSet: DataSet = identifyDataset
  val protocols: Vector[Protocol] = identifyProtocols
  val datasets: Vector[DownloadFromVault] = downloadAzDatasets

  private def identifyDataset = {
    val datasets = downloadDatasets(vaultId)

    val admeDatasets = datasets.filter {
      _.name.startsWith("ADME")
    }

    val azDataSet = datasets.find(_.name == "ADME: AZ Public ChEMBL Data")
    assert(azDataSet.isDefined)
    azDataSet.get
  }

  private def identifyProtocols = {

    val azId = dataSet.id

    val protocols = downloadProtocolsForDataset(vaultId, azId)

    val azProtocolNames = Vector("AZ Human Microsomal Intrinsic Clearance",
      "AZ Human protein binding",
      "AZ Rat hepatocyte intrinsic clearance",
      "AZ human intrinsic clearance less or equal to 10 microL/min/mg",
      "AZ rat hepatocye intrinsic clearance less or equal to 10 microL/min/1E6 cells",
      "AZ human prot bind less or equal to 50%",
      "AZ Guinea Pig protein binding",
      "AZ Dog protein binding",
      "AZ Rat protein binding",
      "AZ Mouse protein binding",
      "AZ human hepatocyte intrinsic clearance")

    protocols.filter(p => azProtocolNames.contains(p.name) && p.category != "Machine Learning Model")
  }

  private def downloadAzDatasets = {

    val azReadoutNames = Set("Human microsomal intrinsic clearance",
      "Protein Binding",
      "AZ rat hepatocyte intrinsic clearance",
      "Score",
      "Protein binding",
      "Intrinsic clearance")

    val azId = dataSet.id

    protocols.map { protocol =>
      val pId = protocol.id
      assert(protocol.runs.length == 1)

      val readouts = protocol.readoutDefinitions.filter(rd => azReadoutNames.contains(rd.name))
      assert(readouts.length == 1)
      val readout = readouts(0)

      new DownloadFromVault(vaultId, Some(azId), readout.name, Some(protocol.id))
    }

  }
}

object ProcessAzAdmeData extends App with HasLogging {

  val azAdmeData = new AzAmdeData

  val results = azAdmeData.protocols.zip(azAdmeData.datasets).flatMap { case (protocol, downloader) =>
    logger.info(s"Downloading for protocol ${protocol.name} readout ${downloader.readoutName} to directory ${downloader.downloadDir.getAbsolutePath}")
    val dt = downloader.downloadFile()

    val regression = new VaultRegressionModel(dt)
    val results = regression.applyRegressors()
    VaultRegressionModel.saveRegressionResults(protocol.name, downloader.downloadDir(), results)
    val bestResult = NamedRegressorValidation(protocol.name, results(0))
    regression.applyLogRegressors() match {
      case Some(logResults) =>
        VaultRegressionModel.saveRegressionResults("log_" + protocol.name, downloader.downloadDir(), results,
          fileNamePrefix = "log_regression_")
        val logBestResult = NamedRegressorValidation("log_" + protocol.name, logResults(0))
        Vector(bestResult, logBestResult)
      case None => Vector(bestResult)
    }
  }

  val arithmeticResults = results.filter(!_.name.startsWith("log_"))
  var file = Util.getProjectFilePath("data/vault/az_regression_plot.png")
  VaultRegressionModel.regressionPlots(arithmeticResults.map(_.regressorValidation), file.getAbsolutePath,
    arithmeticResults.map(_.name), 4)

  val logResults = results.filter(_.name.startsWith("log_"))
  file = Util.getProjectFilePath("data/vault/az_log_regression_plot.png")
  VaultRegressionModel.regressionPlots(logResults.map(_.regressorValidation), file.getAbsolutePath,
    logResults.map(_.name), 4)

  val bestClassificationResults = azAdmeData.protocols.zip(azAdmeData.datasets).map { case (protocol, downloader) =>
    logger.info(s"Downloading for protocol ${protocol.name} readout ${downloader.readoutName} to directory ${downloader.downloadDir.getAbsolutePath}")

    var dt = downloader.downloadFile()
    val threshold = protocol.name match {
      case name if name.toLowerCase.contains("protein binding") => 50
      case name if name.toLowerCase.contains("intrinsic clearance") => 10
      case _ => throw new IllegalArgumentException
    }
    dt = dt.continuousColumnToDiscrete("label", threshold, false, "active")

    val classification = new VaultClassificationModel(dt)
    val results = classification.applyClassifiers("active")
    VaultClassificationModel.saveClassificationResults(protocol.name, downloader.downloadDir(), results)
    new NamedClassifierValidation(protocol.name, results(0))
  }
  file = Util.getProjectFilePath("data/vault/az_classification_plot.png")
  VaultClassificationModel.classificationPlots(bestClassificationResults.map(_.classifierValidation),
    file.getAbsolutePath, bestClassificationResults.map(_.name), 4)
}


class AdmeDataCom {
  val vaultId = 3663 // G2 Research Vault
  val project: Project = identifyProject
  val protocols: Vector[Protocol] = identifyProtocols
  val datasets: Vector[(Protocol, DownloadFromVault)] = downloadAdDatasets

  private def identifyProject = {
    val projects = downloadProjects(vaultId)

    val adProject = projects.find(_.name == "G2 Research ADMEdata.com")
    assert(adProject.isDefined)
    adProject.get
  }

  private def identifyProtocols = {

    val projectId = project.id

    val protocols = downloadProtocolsForProject(vaultId, projectId)

    val adProtocolNames = Vector("Caco-2 Permeability",
      "Protein Binding - Human",
      "Protein Binding - Rat",
      "Rabbit Intestinal Permeability",
      "Equilibrium Solubility @ 5pH Values",
      "Human Blood Plasma Partitioning and Hematocrit")

    protocols.filter(p => adProtocolNames.contains(p.name))
  }

  private def downloadAdDatasets = {

    val adReadoutNames = Set(
      "Caco-2 pH 6.5 AB",
      "Caco-2 pH 6.5 BA",
      "Caco-2 pH 6.5 Efflux Ratio",
      "Caco-2 pH 7.4 AB",
      "Caco-2 pH 7.4 BA",
      "Caco-2 pH 7.4 Efflux Ratio",
      "Human Protein Binding",
      "Rat Protein Binding",
      "Rabbit Duodenum AB",
      "Rabbit Duodenum BA",
      "Rabbit Jejunum AB",
      "Rabbit Jejunum BA",
      "Rabbit Ileum AB",
      "Rabbit Ileum BA",
      "Rabbit Colon AB",
      "Rabbit Colon BA",
      "Rabbit Jejunum Efflux",
      "Rabbit Duodenum Efflux",
      "Rabbit Ileum Efflux",
      "Rabbit Colon Efflux",
      "Human BPP 0.04µM",
      "Human BPP 2µM",
      "Human BPP 10µM",
      "Human BPP/Hematocrit"
    )

    val projectId = project.id

    protocols.flatMap { protocol =>
      val pId = protocol.id
      assert(protocol.runs.length == 1)

      val readouts = protocol.readoutDefinitions.filter(rd => adReadoutNames.contains(rd.name))

      readouts.map { readout =>
        (protocol, new DownloadFromVault(vaultId, None, readout.name, Some(protocol.id)))
      }
    }
  }
}

object ProcessAdmeDataCom extends App with HasLogging {

  val admeDataCom = new AdmeDataCom

  admeDataCom.datasets.foreach { case (protocol, downloader) =>
    logger.info(s"Downloading for protocol ${protocol.name} readout ${downloader.readoutName} to directory ${downloader.downloadDir.getAbsolutePath}")
    downloader.downloadFile()
  }

  val results = admeDataCom.datasets.flatMap { case (protocol, downloader) =>
    logger.info(s"Downloading for protocol ${protocol.name} readout ${downloader.readoutName} to directory ${downloader.downloadDir.getAbsolutePath}")
    val dt = downloader.downloadFile()

    val regression = new VaultRegressionModel(dt)
    val results = regression.applyRegressors()
    VaultRegressionModel.saveRegressionResults(downloader.readoutName, downloader.downloadDir(), results)
    val bestResult = NamedRegressorValidation(downloader.readoutName, results(0))
    regression.applyLogRegressors() match {
      case Some(logResults) =>
        VaultRegressionModel.saveRegressionResults("log_" + downloader.readoutName, downloader.downloadDir(), results,
          fileNamePrefix = "log_regression_")
        val logBestResult = NamedRegressorValidation("log_" + downloader.readoutName, logResults(0))
        Vector(bestResult, logBestResult)
      case None => Vector(bestResult)
    }
  }

  val arithmeticResults = results.filter(!_.name.startsWith("log_"))
  var file = Util.getProjectFilePath("data/vault/adcom_regression_plot.png")
  VaultRegressionModel.regressionPlots(arithmeticResults.map(_.regressorValidation), file.getAbsolutePath,
    arithmeticResults.map(_.name), 5)

  val logResults = results.filter(_.name.startsWith("log_"))
  file = Util.getProjectFilePath("data/vault/adcom_log_regression_plot.png")
  VaultRegressionModel.regressionPlots(logResults.map(_.regressorValidation), file.getAbsolutePath,
    logResults.map(_.name), 5)

  val bestClassificationResults = admeDataCom.datasets.flatMap { case (protocol, downloader) =>
    var dt = downloader.downloadFile()
    val threshold = protocol.name match {
      case name if name.toLowerCase.contains("protein binding") => Some(50)
      case _ => None
    }
    threshold match {
      case Some(t) =>
        dt = dt.continuousColumnToDiscrete("label", t, false, "active")
        val classification = new VaultClassificationModel(dt)
        val results = classification.applyClassifiers("active")
        VaultClassificationModel.saveClassificationResults(downloader.readoutName, downloader.downloadDir(), results)
        Some(NamedClassifierValidation(downloader.readoutName, results(0)))
      case None => None
    }
  }

  file = Util.getProjectFilePath("data/vault/adcom_classification_plot.png")
  VaultClassificationModel.classificationPlots(bestClassificationResults.map(_.classifierValidation),
    file.getAbsolutePath, bestClassificationResults.map(_.name), cols = 2)

}



