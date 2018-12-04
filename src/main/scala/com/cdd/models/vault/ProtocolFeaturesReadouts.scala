package com.cdd.models.vault

import com.cdd.models.datatable.{DataTable, DataTableColumn, DenseVector, SparseVector}
import com.cdd.models.universalmetric.LabelIdentifierModelAndTransformer
import com.cdd.models.utils.{HasLogging, Util}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.cdd.models.bae.AssayNameMatcher.matcher

@SerialVersionUID(1000L)
class FilteredProtocolFeaturesReadouts(datasetInformation: DatasetInformation,
                                       val readoutNamesAndValues: Vector[(String, String)],
                                       protocolReadouts: Vector[MoleculeFeaturesAndReadouts])
  extends ProtocolFeaturesReadouts(datasetInformation, protocolReadouts) with Serializable {

  def equalSplit(other: FilteredProtocolFeaturesReadouts): Boolean = {
    if (protocolReadouts.size != other.protocolReadouts.size)
      return false
    protocolReadouts.zip(other.protocolReadouts).foreach { case (pr1, pr2) =>
      if (pr1.readouts != pr2.readouts)
        return false
    }
    true
  }

  def namesString: String = {
    readoutNamesAndValues.map(t => s"${t._1}:${t._2}").mkString("|")
  }
}

object ProtocolFeaturesReadouts {
  val targetMatchers = Vector(
    matcher("""(?i)^target""".r)
  )

  val assayMatchers = Vector(
    matcher("""(?i)assay description""".r)
  )

  val measurementMatchers = Vector(
    matcher("""(?i)activity type""".r)
  )

  val unitsMatchers = Vector(
    matcher( """(?i)unit""".r)
  )

  def identifyMlReadouts(readoutNamesAndDescriptions: Vector[(String, String)]): Vector[String] = {
    val model = LabelIdentifierModelAndTransformer.modelAndTransformer
    readoutNamesAndDescriptions
      .filter { case (name, description) => model.predict(name, description) }
      .map(_._1)
  }

  def identifySplittingReadouts(readoutNames: Vector[String]): Vector[(String, Vector[String])] = {
    Vector("Assay", "Measurement", "Target", "Units").flatMap { name =>
      val readouts = name match {
        case "Assay" =>
          readoutNames.filter(isAssayReadout)
        case "Measurement" =>
          readoutNames.filter(isMeasurementReadout)
        case "Target" =>
          readoutNames.filter(isTargetReadout)
        case "Units" =>
          readoutNames.filter(isUnitsReadout)
      }

      if (readouts.isEmpty) None else Some(name, readouts)
    }
  }

  private def isAssayReadout(readout: String): Boolean = {
    assayMatchers.exists(_.matchName(readout))
  }

  private def isUnitsReadout(readout: String): Boolean = {
    unitsMatchers.exists(_.matchName(readout))
  }

  private def isMeasurementReadout(readout: String): Boolean = {
    measurementMatchers.exists(_.matchName(readout))
  }

  private def isTargetReadout(readout: String): Boolean = {
    targetMatchers.exists(_.matchName(readout))
  }

  def findByGroupReadoutAndValue(protocolFeaturesReadouts: Vector[ProtocolFeaturesReadouts],
                                 groupNamesAndValues: Vector[(String, String)])
  : Option[FilteredProtocolFeaturesReadouts] = {
    protocolFeaturesReadouts.find {
      case fpfr: FilteredProtocolFeaturesReadouts =>
        fpfr.readoutNamesAndValues.toSet == groupNamesAndValues.toSet
      case _ => false
    }.asInstanceOf[Option[FilteredProtocolFeaturesReadouts]]
  }
}

@SerialVersionUID(1000L)
class ProtocolFeaturesReadouts(val datasetInformation: DatasetInformation, val protocolReadouts: Vector[MoleculeFeaturesAndReadouts])
  extends Serializable with HasLogging {

  import ProtocolFeaturesReadouts._

  def toDataTables(): Vector[VaultDataTable] = {
    val protocolFeaturesReadouts = toGroupedData()
    toDataTables(protocolFeaturesReadouts)
  }

  def toGroupedData(): Vector[ProtocolFeaturesReadouts] = {

    val readoutDefinitions = datasetInformation.protocol.readoutDefinitions
    if (readoutDefinitions == null) {
      logger.warn(s"No readout definitions for dataset ${datasetInformation.dataset.name} protocol ${datasetInformation.protocol.name}")
      datasetInformation.protocol.readoutDefinitions = Array.empty
      return Vector.empty
    }
    val textReadouts = readoutDefinitions
      .filter {
        _.dataType == "Text"
      }
      .map {
        _.name
      }.toVector
    val splittingReadouts = identifySplittingReadouts(textReadouts)
    logger.info(s"Splitting readouts are ${splittingReadouts.mkString("|")}")
    val protocolFeaturesReadouts =
      if (splittingReadouts.isEmpty) {
        logger.info("No Splitting fields using complete protocol data")
        Vector(this)
      } else {

        val readoutGroups = splittingReadouts
          .map((readoutValuesForNames _).tupled)
          .map { case (readoutGroup) =>
            readoutGroup.flatMap { case (readout, values) =>
              values.map { v => (readout, v) }
            }
          }

        val readoutsAndValues = Util.cartesianProduct(readoutGroups)

        readoutsAndValues.foldRight(ArrayBuffer[FilteredProtocolFeaturesReadouts]()) {
          case (groupReadoutsAndValues, filteredProtocolReadouts) =>
            val info = groupReadoutsAndValues.map(t => s"${t._1}:${t._2}").mkString("|")
            logger.info(s"Splitting on readout names and values $info")
            val newSplit = splitOnReadouts(groupReadoutsAndValues.toVector)
            if (newSplit.protocolReadouts.isEmpty)
              logger.info("Split is empty")
            else if (!filteredProtocolReadouts.exists(_.equalSplit(newSplit))) {
              filteredProtocolReadouts.append(newSplit)
              logger.info("adding new split")
            }
            else
              logger.info("split is already present")
            filteredProtocolReadouts
        }.toVector

      }

    protocolFeaturesReadouts
  }

  def toDataTables(protocolFeaturesReadouts: Vector[ProtocolFeaturesReadouts]): Vector[VaultDataTable] = {
    val readoutDefinitions = datasetInformation.protocol.readoutDefinitions
    val numericReadoutsAndDescriptions = readoutDefinitions
      .filter {
        rd => rd.dataType == "Number"
      }
      .map {
        rd => (rd.name, rd.description)
      }
      .toVector
    val mlReadouts = identifyMlReadouts(numericReadoutsAndDescriptions)
    logger.info(s"ML readouts are ${
      mlReadouts.mkString("|")
    }")

    val dfs = protocolFeaturesReadouts.flatMap {
      pfr =>
        mlReadouts.flatMap {
          readout =>
            pfr.toDataTable(readout) match {
              case Some((dt, commonTerms)) =>
                pfr match {
                  case pfr: FilteredProtocolFeaturesReadouts =>
                    logger.info(s"Creating vault datatable readout $readout group readouts and values ${
                      pfr.namesString
                    } size ${
                      dt.length
                    }")
                    val vdt = new VaultDataTable(datasetInformation, readout, Some(pfr.readoutNamesAndValues), commonTerms, dt)
                    Some(vdt)
                  case pfr: ProtocolFeaturesReadouts =>
                    logger.info(s"Creating vault datatable readout $readout size ${
                      dt.length
                    }")
                    val vdt = new VaultDataTable(datasetInformation, readout, None, commonTerms, dt)
                    Some(vdt)
                }
              case None =>
                None
            }
        }
    }

    dfs
  }

  private def readoutAndMetaData(readouts: Vector[Option[ReadoutValue[_]]], valueIndex: Int):
  Option[(ReadoutValue[Double], Map[String, Any])] = {
    val valueReadoutOpt = readouts(valueIndex)
    if (valueReadoutOpt.isEmpty)
      return None
    val valueReadout = valueReadoutOpt.get.asInstanceOf[ReadoutValue[Double]]
    val metaPairs = readouts.zipWithIndex.foldRight(mutable.Map[String, Any]()) {
      case ((r, i), m) =>
        if (i != valueIndex && r.isDefined) {
          val readout = r.get
          val name = datasetInformation.protocol.readoutDefinitions.find {
            _.id == readout.readoutDefinitionId
          }.get.name
          m(name) = readout.value
        }
        m
    }
    Some((valueReadout, metaPairs.toMap))
  }

  def toDataTable(readoutName: String): Option[(DataTable, Map[String, Any])] = {
    val readoutIndex = datasetInformation.protocol.readoutDefinitions.indexWhere(_.name == readoutName)

    val data = protocolReadouts
      .map {
        pr =>
          val values = pr.readouts.flatMap(readoutAndMetaData(_, readoutIndex))
          (pr.molecule, values)
      }
      .filter {
        case (m, v) =>
          m.isDefined && v.nonEmpty
      }
      .map {
        case (m, v) =>
          assert(m.isDefined)
          val (modifier, value, logValue, commonTerms) = DownloadFromVault.readoutsAndTermsToSingleValue(v)
          (m.get, value, modifier, logValue, commonTerms)
      }
    if (data.isEmpty)
      return None

    val ids = ArrayBuffer[Int]()
    val rdkitFps = ArrayBuffer[SparseVector]()
    val rdkitDescs = ArrayBuffer[DenseVector]()
    val cdkFps = ArrayBuffer[SparseVector]()
    val cdkDescs = ArrayBuffer[DenseVector]()
    val labels = ArrayBuffer[Double]()
    val modifiers = ArrayBuffer[String]()
    val rdkitSmiles = ArrayBuffer[String]()
    val cdkSmiles = ArrayBuffer[String]()
    val logLabels = ArrayBuffer[Option[Double]]()
    val allTerms = ArrayBuffer[Map[String, Any]]()

    for ((mol, value, modifier, logModifier, terms) <- data) {
      ids.append(mol.id)
      rdkitFps.append(mol.rdkitFp)
      rdkitDescs.append(mol.rdkitDesc)
      cdkFps.append(mol.cdkFp)
      cdkDescs.append(mol.rdkitDesc)
      labels.append(value)
      modifiers.append(modifier.toString)
      rdkitSmiles.append(mol.rdkitSmiles)
      cdkSmiles.append(mol.cdkSmiles)
      logLabels.append(logModifier)
      allTerms.append(terms)
    }


    val columns = Vector(
      DataTableColumn.fromVector("no", Vector.range(0, ids.length)),
      DataTableColumn.fromVector("id", ids.toVector),
      DataTableColumn.fromVector("fingerprints_RDKit_FCFP6", rdkitFps.toVector),
      DataTableColumn.fromVector("rdkit_descriptors", rdkitDescs.toVector),
      DataTableColumn.fromVector("fingerprints_CDK_FCFP6", cdkFps.toVector),
      DataTableColumn.fromVector("cdk_descriptors", cdkDescs.toVector),
      DataTableColumn.fromVector("rdkit_smiles", rdkitSmiles.toVector),
      DataTableColumn.fromVector("cdk_smiles", cdkSmiles.toVector),
      DataTableColumn.fromVector("label", labels.toVector),
      new DataTableColumn[Double]("log_label", logLabels.toVector, classOf[Double])
    )

    val commonTerms = Util.commonKeysAndValues(allTerms.toVector)
    Some((new DataTable(columns), commonTerms))
  }

  def readoutValuesForNames(readoutCategory: String, readoutNames: Vector[String]): Vector[(String, Vector[String])] = {
    readoutNames.map {
      name =>
        (name, readoutValuesForName(name))
    }
  }

  def readoutValuesForName(readoutName: String): Vector[String] = {
    val readoutDefinitionId = datasetInformation.protocol.readoutDefinitions.find(_.name == readoutName).get.id
    val readoutIndex = datasetInformation.protocol.readoutDefinitions.indexWhere(_.name == readoutName)

    protocolReadouts
      .flatMap {
        mfr =>
          mfr.readouts.flatMap {
            r =>
              val testReadout = r(readoutIndex)
              testReadout match {
                case Some(rv) =>
                  assert(rv.readoutDefinitionId == readoutDefinitionId)
                  require(rv.clazz == classOf[String])
                  Some(rv.value.asInstanceOf[String])
                case None => None
              }
          }
      }
      .distinct
  }

  def splitOnReadouts(readoutNamesAndValues: Vector[(String, String)]): FilteredProtocolFeaturesReadouts = {
    val readoutIndexesAndValues = readoutNamesAndValues.map {
      case (readoutName, readoutValue) =>
        val readoutDefinitionId = datasetInformation.protocol.readoutDefinitions.find(_.name == readoutName).get.id
        val readoutIndex = datasetInformation.protocol.readoutDefinitions.indexWhere(_.name == readoutName)
        (readoutIndex, readoutValue)
    }
    val filteredProtocolReadouts = protocolReadouts
      .map {
        mfr =>
          val matchingReadouts = mfr.readouts.filter {
            molReadouts =>
              readoutIndexesAndValues.forall {
                case (readoutIndex, readoutValue) =>
                  matchingRow(readoutIndex, readoutValue, molReadouts)
              }
          }
          new MoleculeFeaturesAndReadouts(mfr.moleculeSummary, mfr.molecule, matchingReadouts)
      }
      .filter(_.readouts.nonEmpty)
    new FilteredProtocolFeaturesReadouts(datasetInformation, readoutNamesAndValues, filteredProtocolReadouts)
  }

  private def matchingRow(readoutIndex: Int, readoutValue: String, row: Vector[Option[ReadoutValue[_]]]): Boolean = {
    val testReadout = row(readoutIndex)
    testReadout match {
      case Some(rv) =>
        require(rv.clazz == classOf[String])
        rv.value.asInstanceOf[String] == readoutValue
      case None => false
    }
  }
}
