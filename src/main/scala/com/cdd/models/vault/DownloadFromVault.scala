package com.cdd.models.vault


import java.io.File

import com.cdd.models.datatable.{CdkTableBuilder, DataTable, DataTableColumn, RdkitTableBuilder}
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitUtils
import com.cdd.models.utils.{LoadsRdkit, Modifier, Util}
import com.cdd.models.vault.VaultDownload._
import org.openscience.cdk.silent.SilentChemObjectBuilder
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator, SmilesParser}
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DownloadFromVault extends LoadsRdkit {

  type MoleculeReadouts = Vector[Vector[Option[ReadoutValue[_]]]]

  private class ArgConf(args: Seq[String]) extends ScallopConf(args) {

    val vault = opt[Int](required = true, default = Some(4835))
    val dataset = opt[Int]()
    val readoutName = opt[String](required = true, default = Some("logEC50"))
    val protocol = opt[Int](default = Some(34630))


    /*
    val vault = opt[Int](required = true, default = Some(4724))
    val dataset = opt[Int]()
    val readoutName = opt[String](required = true, default = Some("pMIC_Ave"))
    val protocol = opt[Int](default = Some(32340))
*/

    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new ArgConf(args)

    val vaultId = conf.vault()
    val datasetId = if (conf.dataset.isDefined) Some(conf.dataset()) else None
    val readoutName = conf.readoutName()
    val protocolId = if (conf.protocol.isDefined) Some(conf.protocol()) else None

    val downloader = new DownloadFromVault(vaultId, datasetId, readoutName, protocolId)
    val dt = downloader.downloadFile()

    val regression = new VaultRegressionModel(dt)
    val results = regression.applyRegressors()
    VaultRegressionModel.saveRegressionResults("Example", downloader.downloadDir(), results)

    val logResults = regression.applyLogRegressors()
    if (logResults.isDefined) {
      VaultRegressionModel.saveRegressionResults("Example", downloader.downloadDir(), logResults.get, "log_regression_")
    }

  }

  def readoutsAndTermsToSingleValue(readoutsAndTerms: Vector[(ReadoutValue[Double], Map[String, Any])]):
  (Modifier, Double, Option[Double], Map[String, Any]) = {
    val commonTerms = Util.commonKeysAndValues(readoutsAndTerms.map(_._2))
    val readouts = readoutsAndTerms.map(_._1)
    val (modifier, value, logValue) = readoutsToSingleValue(readouts)
    (modifier, value, logValue, commonTerms)
  }

  def readoutsToSingleValue(readouts: Vector[ReadoutValue[Double]]): (Modifier, Double, Option[Double]) = {
    val upperValues = readouts.filter(_.modifier.greater()).map(_.value)
    val lowerValues = readouts.filter(_.modifier.less()).map(_.value)
    val equalValues = readouts.filter(_.modifier == Modifier.EQUAL).map(_.value)

    if (equalValues.nonEmpty) {
      val logValue =
        if (equalValues.forall(_ > .0)) {
          val logSum = equalValues.map(Math.log10).sum
          val logMean = logSum / equalValues.length.toDouble
          Some(logMean)
        } else {
          None
        }
      (Modifier.EQUAL, equalValues.sum / equalValues.length.toDouble, logValue)
    } else if (upperValues.nonEmpty) {
      require(lowerValues.isEmpty)
      val max = upperValues.max
      val logMax = if (max>0) Some(Math.log10(max)) else None
      (Modifier.GREATER_THAN, max, logMax)
    } else {
      require(lowerValues.nonEmpty)
      require(upperValues.isEmpty)
      val min = lowerValues.min
      val logMin = if (min > 0) Some(Math.log10(min)) else None
      (Modifier.LESS_THAN, min, logMin)
    }
  }

  def downloadFromVaultDataset(vaultId: Int, datasetName: String, readoutName: String, protocolName: String): DownloadFromVault = {
    val dataset = downloadDatasets(vaultId).find(_.name == datasetName) match {
      case Some(d) => d
      case None => throw new IllegalArgumentException(s"Unable to find dataset $datasetName")
    }
    val protocol = downloadProtocolsForDataset(vaultId, dataset.id).find(_.name == protocolName) match {
      case Some(p) => p
      case None => throw new IllegalArgumentException(s"Unable to find protocol $protocolName")
    }
    new DownloadFromVault(vaultId, Some(dataset.id), readoutName, Some(protocol.id))
  }

  def downloadFromVaultProject(vaultId: Int, projectName: String, readoutName: String, protocolName: String): DownloadFromVault = {
    val project = downloadProjects(vaultId).find(_.name == projectName) match {
      case Some(p) => p
      case None => throw new IllegalArgumentException(s"Unable to find project $projectName")
    }
    val protocol = downloadProtocolsForProject(vaultId, project.id).find(_.name == protocolName) match {
      case Some(p) => p
      case None => throw new IllegalArgumentException(s"Unable to find protocol $protocolName")
    }
    new DownloadFromVault(vaultId, None, readoutName, Some(protocol.id))
  }

  def downloadReadoutsForProtocol(vaultId: Int, protocol: Protocol, datasetId: Option[Int] = None, readoutNames:
  Option[Vector[String]] = None): Map[Int, MoleculeReadouts] = {

    val readoutDefinitions = {
      readoutNames match {
        case Some(names) =>
          val rds = protocol.readoutDefinitions.filter { rd =>
            names.contains(rd.name)
          }
          if (rds.length != names.length) {
            val availableReadouts = protocol.readoutDefinitions.map(_.name).mkString("|")
            throw new IllegalArgumentException(s"Unable to find all readout definitions ${readoutNames.mkString("|")} available readouts: $availableReadouts")
          }
          rds
        case None => protocol.readoutDefinitions match {
          case null => Array[ReadoutDefinition]()
          case rds => rds.filter(rd => rd.dataType == "Number" || rd.dataType == "Text")
        }
      }
    }.toVector

    readoutDefinitions.foreach(rd => require(rd.dataType == "Number" || rd.dataType == "Text"))
    val readoutDefinitionIds = readoutDefinitions.map(_.id)

    val readoutData = downloadProtocolData(vaultId, protocol, datasetId = datasetId)
    readoutData.map { rd =>
      val molecule = rd.protocolData.molecule
      val readoutValues = readoutDefinitionIds.map { rid =>
        rd.readoutValues.find(r => r.readoutDefinitionId == rid)
      }
      (molecule, readoutValues)
    }
      .foldLeft(mutable.Map[Int, ArrayBuffer[Vector[Option[ReadoutValue[_]]]]]()) { case (map, (moleculeId, readoutValues)) =>
        val readouts = readoutValues.map {
          case Some(dv) if dv.clazz == classOf[Double] => Some(dv)
          case Some(sv) if sv.clazz == classOf[String] =>
            val value = sv.asInstanceOf[ReadoutValue[String]].value
            Try {
              value.toDouble
            }.toOption match {
              case Some(dv) =>
                val rv = new ReadoutValue[Double](classOf[Double], sv.protocolId, sv.readoutDefinitionId, dv, sv.modifier)
                Some(rv)
              case None => Some(sv)
            }
          case None => None
          case _ => throw new IllegalArgumentException
        }.toVector
        map.getOrElseUpdate(moleculeId, new ArrayBuffer[Vector[Option[ReadoutValue[_]]]]()).append(readouts)
        map
      }.map { case (moleculeId, readouts) => moleculeId -> readouts.toVector }.toMap

  }
}

class DownloadFromVault(val vaultId: Int, val datasetId: Option[Int], val readoutName: String, val protocolId: Option[Int]) {

  require(datasetId.isDefined || protocolId.isDefined)

  override def toString: String = {
    var baseName = s"v_${vaultId}_r_$readoutName"
    if (datasetId.isDefined)
      baseName += s"_d_${datasetId.get}"
    if (protocolId.isDefined)
      baseName += s"_p_${protocolId.get}"
    baseName
  }

  def downloadDir(): File = {
    var baseName = toString()
    Util.getProjectFilePath(s"data/vault/$baseName")
  }

  def downloadFile(overwrite: Boolean = false): DataTable = {
    val dir = downloadDir()
    val csvFile = new File(dir, "data_table.csv.gz")
    if (!overwrite && csvFile.exists()) {
      return DataTable.loadFile(csvFile.getAbsolutePath)
    }

    var protocol: Protocol = null
    if (datasetId.isDefined) {
      val protocols = downloadProtocolsForDataset(vaultId, datasetId.get)
      protocol = protocolId match {
        case Some(id) => protocols.find(_.id == id) match {
          case None => throw new IllegalArgumentException(s"Unable to find protocol id $id")
          case Some(p) => p
        }
        case None =>
          if (protocols.length == 1) {
            protocols(0)
          }
          else {
            throw new IllegalArgumentException(s"Multiple protocols for dataset")
          }
      }
    }
    else if (protocolId.isDefined) {
      protocol = downloadProtocol(vaultId, protocolId.get)
    }
    else {
      throw new IllegalStateException()
    }
    require(protocol != null)
    if (!dir.exists())
      dir.mkdirs()

    val moleculeData = DownloadFromVault.downloadReadoutsForProtocol(vaultId, protocol, datasetId, Some(Vector(readoutName)))
      .map { case (moleculeId, allReadouts) =>
        val readouts = allReadouts.flatMap { rr =>
          require(rr.length == 1)
          rr(0).asInstanceOf[Option[ReadoutValue[Double]]]
        }
        moleculeId -> DownloadFromVault.readoutsToSingleValue(readouts)
      }

    import com.cdd.models.utils.VectorUtil._

    val (moleculeIds, modifiers, labels, logLabels) = moleculeData.map {
      case (molecule_id, (modifier, label, logLabel)) =>
        (molecule_id, modifier, label, logLabel)
    }.toVector.unzip4

    val molecules = downloadMolecules(vaultId, moleculeIds, datasetId)
    assert(molecules.length == moleculeIds.length)
    val smilesMap = molecules.map { m =>
      val smi = m.smiles match {
        case s: String =>
          RdkitUtils.smilesToMol(s) match {
            case Some(mol) =>
              val smi = mol.MolToSmiles(true)
              assert(smi.nonEmpty)
              Some(smi)
            case None => None
          }
        case _ => None // matches null
      }
      (m.id, smi)
    }.toMap
    val smiles = moleculeIds.map { id =>
      assert(smilesMap.contains(id))
      smilesMap(id)
    }

    val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
    val smilesGenerator = new SmilesGenerator(SmiFlavor.Absolute)
    val cdkSmilesMap = molecules.map { m =>
      val smi = m.smiles match {
        case s: String =>
          val mol = smilesParser.parseSmiles(s)
          val smi = smilesGenerator.create(mol)
          Some(smi)
        case _ => None
      }
      (m.id, smi)
    }.toMap
    val cdkSmiles = moleculeIds.map { id =>
      assert(smilesMap.contains(id))
      smilesMap(id)
    }

    val cols = Vector(
      DataTableColumn.fromVector("id", moleculeIds),
      DataTableColumn.fromVector("label", labels),
      new DataTableColumn[Double]("log_label", logLabels),
      new DataTableColumn[String]("rdkit_smiles", smiles),
      new DataTableColumn[String]("cdk_smiles", cdkSmiles),
      DataTableColumn.fromVector("modifier", modifiers.map(m => m.toString))
    )

    var dt = new DataTable(cols)
    var rdkitBuilder = new RdkitTableBuilder()
      .setDataTable(dt)
      .addDescriptorColumns()
      .addFingerprintColumn(RdkitFingerprintClass.FCFP6)
    dt = rdkitBuilder.getDt

    var cdkBuilder = new CdkTableBuilder()
      .setDataTable(dt)
      .addDescriptorColumns()
      .addFingerprintColumn(CdkFingerprintClass.FCFP6)
    dt = cdkBuilder.getDt

    dt.exportToFile(csvFile.getAbsolutePath)
    dt
  }

}


