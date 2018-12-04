package com.cdd.models.vault

import java.io.{File, FileFilter, FileWriter}

import VaultDownload._
import com.cdd.models.datatable.DataTable
import com.cdd.models.utils.{HasLogging, LoadsRdkit, Util}
import com.cdd.models.vault.DownloadFromVault.MoleculeReadouts
import com.opencsv.CSVWriter
import org.apache.commons.httpclient.HttpException
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.rogach.scallop.ScallopConf

object DatasetInformation {
  val downloadDir: File = Util.getProjectFilePath("data/vault/protocol_molecule_downloads/")
  val featuresDir: File = Util.getProjectFilePath("data/vault/protocol_molecule_features/")
}

@SerialVersionUID(-442845433962364956L)
case class DatasetInformation(vaultId: Int, dataset: DataSet, protocol: Protocol) extends Serializable {

  import DatasetInformation._

  def base: String = s"v_${vaultId}_d_${dataset.name}_p_${protocol.name}".replace("/", "")

  def resultsDir: File = {
    val base = s"v_${vaultId}_p_${protocol.id}_${protocol.name}".replace('/', '_')
    Util.getProjectFilePath(s"data/vault/protocol_molecule_features/$base")
  }

  def downloadFile: File = new File(downloadDir, base + ".obj")

  def featuresFile: File = new File(featuresDir, base + ".obj")

  def information: String = {
    s"dataset ${dataset.name} id ${dataset.id} protocol ${protocol.name} id ${protocol.id}"
  }
}

object VaultReadouts extends HasLogging {
  val informationFile = Util.getProjectFilePath("data/vault/datasetInformation.obj")
  lazy val protocolInformationList = loadProtocolInformationList()

  def downloadDatasetInformation(): Vector[DatasetInformation] = {
    val vaultId = 1
    var datasets = downloadDatasets(1)
    //datasets = datasets.slice(0, 3)

    datasets.flatMap { d =>
      logger.info(s"Downloading protocols for dataset ${d.name}")
      val protocols = downloadProtocolsForDataset(vaultId, d.id)
      protocols.map { p =>
        if (p.readoutDefinitions != null) {
          p.readoutDefinitions.foreach { rd =>
            try {
              val nReadouts = determineProtocolReadoutCount(vaultId, p, None, Some(d.id))
              rd.nReadouts = nReadouts
              logger.info(s"Readout ${rd.name} has $nReadouts readouts")
            } catch {
              case ex: HttpException =>
                logger.warn(s"Http Exception: ${ex.getMessage} for readout definition ${rd.name}")
            }
          }
        }
        DatasetInformation(vaultId, d, p)
      }
    }.toVector
  }

  def saveProtocolInformation() = {
    val datasets = downloadDatasetInformation()
    Util.serializeObject(informationFile.getAbsolutePath, datasets)
  }


  private def loadProtocolInformationList(): Vector[DatasetInformation] = {
    Util.withSerializedFile(informationFile) { () =>
      downloadDatasetInformation()
    }
  }

  def datasetInformationForObjectFile(objectFile: String): Option[DatasetInformation] = {
    val file = new File(objectFile)
    val testName = FilenameUtils.removeExtension(file.getName())
    protocolInformationList.find(_.base == testName)
  }

  def datasetInformationForProtocolFile(fileInProtocolDirOrProtocolDir: String): Option[DatasetInformation] = {
    val file = new File(fileInProtocolDirOrProtocolDir)
    val testName = if (file.isDirectory) file.getName else file.getParentFile.getName
    protocolInformationList.find(_.resultsDir.getName == testName)
  }

  def _groupValues(func: (DatasetInformation) => String) = {
    val counts = protocolInformationList.groupBy(func).mapValues(_.length)
    counts.values.max
  }

  def validateDatasetInformation(): Unit = {
    val datasets = protocolInformationList

    // check protocols only appear once by id in the public vault
    val maxProtocolCounts = _groupValues((di: DatasetInformation) => di.protocol.id.toString)
    require(maxProtocolCounts == 1)

    // check base name is unique over protocols
    val maxBaseCounts = _groupValues((di: DatasetInformation) => di.base)
    require(maxBaseCounts == 1)

    // check protocol directory is unique
    val maxDirCounts = _groupValues((di: DatasetInformation) => di.resultsDir.getAbsolutePath)
    require(maxDirCounts == 1)

    // check protocols only appear once by name in the public vault- this is not the case
    val maxProtocolNameCounts = _groupValues((di: DatasetInformation) => di.protocol.name)
    //require(maxProtocolNameCounts == 1)
  }

  def processInformation() = {
    val information = protocolInformationList
    val summaryFile = Util.getProjectFilePath("data/vault/readout_fields.csv")


    Util.using(new FileWriter(summaryFile)) { fh =>
      val csvWriter = new CSVWriter(fh)
      csvWriter.writeNext(Array("DataSet", "Protocol", "Readout", "Type", "Description", "Count", "UseReadout"))

      information.foreach { info =>

        val dataSet = info.dataset
        val protocol = info.protocol
        if (protocol.readoutDefinitions != null)
          protocol.readoutDefinitions.foreach { rd =>
            val useReadout = if (rd.dataType == "Number" && learningReadout(rd.name.toLowerCase())) 1 else 0
            logger.info(s"dataset ${dataSet.name} protocol ${protocol.name} readout ${rd.name} type ${rd.dataType} description ${rd.description} count ${rd.nReadouts} $useReadout")
            csvWriter.writeNext(Array(dataSet.name, protocol.name, rd.name, rd.dataType, rd.description, rd.nReadouts.toString, useReadout.toString));
          }
      }
    }
  }


  private def learningReadout(readoutName: String) = {
    readoutName match {
      case s if s.contains("ec50") => true
      case s if s.contains("ic50") => true
      case s if s.contains("um") => true
      case s if s.contains("%") => true
      case s if s.contains("intrinsic clearance") => true
      case s if s.contains("protein binding") => true
      case s if s.contains("solubility") => true
      case s if s.contains("activity") => true
      case s if s.contains("results") => true
      case s if s.contains("inhibition") => true
      case s if s.contains("value") => true
      case _ => false
    }
  }
}

@SerialVersionUID(7719955056003688572L)
class MoleculeSummary(val id: Int, val smiles: String) extends Serializable

@SerialVersionUID(1000L)
class MoleculeAndReadouts(val molecule: MoleculeSummary, val readouts: MoleculeReadouts) extends Serializable

@SerialVersionUID(-1137891237566976723L)
class ProtocolReadouts(val information: DatasetInformation, val protocolReadouts: Vector[MoleculeAndReadouts]) extends Serializable

@SerialVersionUID(1000L)
class MoleculeFeaturesAndReadouts(val moleculeSummary: MoleculeSummary, val molecule: Option[MoleculeAndFeatures],
                                  val readouts: MoleculeReadouts) extends Serializable


object SaveDatasetInformation extends App {
  VaultReadouts.saveProtocolInformation()
}

object ProcessDatasetInformation extends App {
  VaultReadouts.processInformation()
}

object ValidateDatasetInformation extends App {
  VaultReadouts.validateDatasetInformation()
}

trait DatasetAndProtocolArgs {

  private class ArgConf(args: Seq[String]) extends ScallopConf(args) {
    //val vault = opt[Int](required=true, default=Some(1))
    val dataset = opt[Int]()
    val protocol = opt[Int]()
    val remove = opt[Boolean](default = Some(false))

    verify()
  }

  def argsToDatasetAndProtocol(args: Seq[String]): (Option[Int], Option[Int], Boolean) = {
    val conf = new ArgConf(args)
    val datasetId = if (conf.dataset.isDefined) Some(conf.dataset()) else None
    val protocolId = if (conf.protocol.isDefined) Some(conf.protocol()) else None
    (datasetId, protocolId, conf.remove())
  }

  def processDataset(datasetId: Option[Int], protocolId: Option[Int], information: DatasetInformation): Boolean = {
    if (datasetId.isDefined && information.dataset.id != datasetId.get)
      false
    else if (protocolId.isDefined && information.protocol.id != protocolId.get)
      false
    else
      true
  }

}

object DownloadPublicProtocols extends HasLogging with DatasetAndProtocolArgs {

  import VaultReadouts._

  def main(args: Array[String]): Unit = {
    val settings = argsToDatasetAndProtocol(args)
    download _ tupled settings
  }

  private def download(datasetId: Option[Int], protocolId: Option[Int], remove: Boolean): Unit = {
    val information = protocolInformationList
    information
      .filter { datasetInformation =>
        processDataset(datasetId, protocolId, datasetInformation)
      }.sortBy(_.dataset.id)
      .foreach { datasetInformation =>
        val vaultId = datasetInformation.vaultId
        val protocol = datasetInformation.protocol
        val dataset = datasetInformation.dataset

        val file = datasetInformation.downloadFile
        val fileName = file.getAbsolutePath
        logger.info(s"Retrieving vault data for dataset ${dataset.id} : ${dataset.name} protocol ${protocol.name}")
        if (remove && file.exists()) {
          logger.info("Removing existing results")
          require(file.delete())
        }
        if (!file.exists()) {
          try {
            val protocolReadouts = DownloadFromVault.downloadReadoutsForProtocol(vaultId, protocol, Some(dataset.id))
            val moleculeIds = protocolReadouts.keys.toVector
            val molecules = downloadMolecules(vaultId, moleculeIds, Some(dataset.id)).map { m =>
              new MoleculeSummary(m.id, m.smiles)
            }
            require(moleculeIds.length == molecules.length)
            val moleculesAndReadouts = molecules.map { mol =>
              new MoleculeAndReadouts(mol, protocolReadouts(mol.id))
            }
            logger.info(s"saving to data file $fileName")
            Util.serializeObject(file.getAbsolutePath, new ProtocolReadouts(datasetInformation, moleculesAndReadouts))
          } catch {
            case ex: HttpException => logger.warn(s"Failed to download data: http exception $ex")
          }
        } else {
          logger.info(s"data already downloaded to $fileName")
        }
      }
  }
}

object AddMolecularFeaturesToProtocols extends DatasetAndProtocolArgs with HasLogging with LoadsRdkit {

  import VaultReadouts._

  def main(args: Array[String]): Unit = {
    val settings = argsToDatasetAndProtocol(args)
    addFeatures _ tupled (settings)
  }

  private def addFeatures(datasetId: Option[Int], protocolId: Option[Int], remove: Boolean): Unit = {
    val information = protocolInformationList
    information
      .filter { datasetInformation =>
        processDataset(datasetId, protocolId, datasetInformation)
      }
      .foreach { datasetInformation =>
        val protocol = datasetInformation.protocol
        val dataset = datasetInformation.dataset
        val file = datasetInformation.downloadFile
        val fileName = file.getAbsolutePath
        val newFile = datasetInformation.featuresFile
        if (newFile.exists() && remove) {
          logger.info("Removing existing results")
          require(newFile.delete())
        }
        if (file.exists() && !newFile.exists()) {
          val protocolReadouts = Util.deserializeObject[ProtocolReadouts](fileName)
          logger.info(s"Processing dataset ${dataset.name} protocol ${protocol.name}")
          val molsIn = protocolReadouts.protocolReadouts.map(_.molecule)
          val molsWithFeatures = MoleculeAndFeatures.buildMolecules(molsIn)
          val protocolReadoutsWithFeatures = protocolReadouts.protocolReadouts.zip(molsWithFeatures).map {
            case (molAndReadout, molWithFeatures) =>
              new MoleculeFeaturesAndReadouts(molAndReadout.molecule, molWithFeatures, molAndReadout.readouts)
          }
          val protocolFeaturesReadouts = new ProtocolFeaturesReadouts(datasetInformation, protocolReadoutsWithFeatures)
          Util.serializeObject(newFile.getAbsolutePath, protocolFeaturesReadouts)
          logger.info(s"saved to data file ${newFile.getAbsolutePath}")
        }
      }
  }
}

object ListDownloadedProtocolsApp extends App with HasLogging {

  for (protocolFile <- downloadedProtocolFiles()) {
    val protocolReadouts = Util.deserializeObject[ProtocolReadouts](protocolFile)
    logger.info(s"Dataset: ${protocolReadouts.information.information} Size: ${protocolReadouts.protocolReadouts.length}")
  }

  private def downloadedProtocolFiles(): Array[String] = {
    val fileFilter = new WildcardFileFilter("v_*_d_*.obj").asInstanceOf[FileFilter]
    DatasetInformation.downloadDir
      .listFiles(fileFilter)
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .filter(_.endsWith(".obj"))
  }

}
