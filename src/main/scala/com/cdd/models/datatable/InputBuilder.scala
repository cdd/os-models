package com.cdd.models.datatable

import com.cdd.models.molecule.CdkDataTableBuilder
import com.cdd.models.utils.{HasLogging, SdfileActivityField, TestSdfileProperties}
import org.RDKit.RWMol
import org.apache.log4j.LogManager
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

trait TableBuilder {
  protected var dt: Option[DataTable] = None

  def getDt: DataTable = dt.get

}

case class MoleculeInfo(no: Int, smiles: String, name: Option[String])

/**
  * Base class for building dataframes of ML information from SDF files
  */
abstract class InputBuilder[B <: InputBuilder[B, M], M]
(val path: String, activityFields: Vector[SdfileActivityField], val resource: Boolean, val addActivities: Boolean,
 val prefix: String, val nameField: Option[String])
  extends TableBuilder with HasLogging {


  protected def addFingerprintColumns(): this.type

  protected def addDescriptorColumns(): this.type

  protected def activityFieldValue(moleculeNo: Int, field: String): Option[String]

  protected val molecules: Seq[Option[M]] = loadMolecules()

  def noMolecules(): Int = molecules.length

  protected def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo]

  protected def loadMolecules(): Seq[Option[M]]

  def build(useSdActivityLabels: Boolean = false): DataTable = {
    buildDataFrame(useSdActivityLabels)
    addFingerprintColumns()
    addDescriptorColumns()
    dt.get
  }

  def buildDataFrame(useSdActivityLabels: Boolean = false): this.type = {

    val nBase = nameField match {
      case Some(_) => 3
      case None => 2
    }
    val nColumns = if (addActivities) nBase + activityFields.size * 2 else nBase
    var columnData = ArrayBuffer[ArrayBuffer[Option[Any]]]()
    0 until nColumns foreach (_ => columnData.append(ArrayBuffer[Option[Any]]()))

    logger.info("building base datatable")
    (0 until noMolecules()).foreach { index =>
      columnData(0) += Some(index)
      val info = moleculeInfo(index)
      columnData(1) += info.map(_.smiles)
      if (nameField.isDefined) {
        columnData(2) += info.flatMap(_.name)
      }
      if (addActivities) {
        activityFields.zipWithIndex.foreach { case (activityField, no) =>
          val field = activityField.fieldName
          val fieldValue = if (info.isDefined) activityFieldValue(index, field) else None
          if (fieldValue.isDefined && !fieldValue.get.isEmpty) {
            val doubleValue = activityFields(no).fieldToActivity(fieldValue.get)
            if (doubleValue.isEmpty) {
              logger.warn(s"unable to get float value from field ${fieldValue.get}")
            }
            columnData(nBase + no * 2) += fieldValue
            columnData(nBase + no * 2 + 1) += doubleValue
          }
          else {
            columnData(nBase + no * 2) += None
            columnData(nBase + no * 2 + 1) += None
          }
        }
      }
    }

    logger.warn("Built rows")
    val columns = ArrayBuffer[DataTableColumn[_]]()
    columns += new DataTableColumn[Any]("no", columnData(0).toVector)
    columns += new DataTableColumn[Any](s"${prefix}_smiles", columnData(1).toVector)
    if (nameField.isDefined)
      columns += new DataTableColumn[Any](s"${prefix}_${nameField.get}", columnData(2).toVector)

    if (addActivities) {
      logger.warn("Adding activities")
      activityFields.zipWithIndex.foreach { case (activityField, no) =>
        var title = if (useSdActivityLabels)
          activityField.fieldName + "_input"
        else
          no match {
            case 0 => "activity_string"
            case _ => s"activity_string_${no + 1}"
          }
        val stringValues = columnData(nBase + no * 2).toVector
        if (stringValues.flatten.length == 0)
          throw new IllegalArgumentException(s"No values in SD file for field ${activityField.fieldName}")
        columns += new DataTableColumn[Any](title, stringValues)

        title = if (useSdActivityLabels)
          activityField.fieldName
        else
          no match {
            case 0 => "activity_value"
            case _ => s"activity_value_${no + 1}"
          }
        val doubleValues = columnData(nBase + no * 2 + 1).toVector
        if (doubleValues.flatten.length == 0)
          throw new IllegalArgumentException(s"No transformed values in SD file for field ${activityField.fieldName}")
        columns += new DataTableColumn[Any](title, doubleValues)
      }
    }

    logger.warn("Creating datatable")
    dt = Some(new DataTable(columns.toVector))
    this
  }

}

object InputBuilder extends HasLogging {

  def mergeCdkAndRdkit(cdkDt: DataTable, rdkitDt: DataTable): DataTable = {

    // Join them
    require(rdkitDt.length == cdkDt.length)
    cdkDt.column("no").toInts().zip(rdkitDt.column("no").toInts()).foreach { case (no1, no2) => require(no1 == no2) }

    val noCompounds = rdkitDt.length
    val (noMissing, noMismatches) =
      cdkDt.column("cdk_smiles").values.zip(rdkitDt.column("rdkit_smiles").values).foldLeft((0, 0)) {
        case ((noMissing, noMismatches), t) =>
          t match {
            case (Some(smiles1: String), Some(smiles2: String)) =>
              val m1 = RWMol.MolFromSmiles(smiles1)
              val m2 = RWMol.MolFromSmiles(smiles2)
              val matched = (m1, m2) match {
                case (null, null) => smiles1 == smiles2
                case (null, _) => false
                case (_, null) => false
                case (_, _) => m1.hasSubstructMatch(m2)
              }
              if (!matched) {
                val msg = s"CDK smiles ${smiles1} and RDKit smiles ${smiles2} do not match!"
                logger.error(msg)
                (noMissing, noMismatches + 1)
              } else
                (noMissing, noMismatches)
            case _ =>
              logger.warn("missing smiles!")
              (noMissing + 1, noMismatches)
          }
      }

    // a small proportion of mis-matches is acceptable (assume they are artifacts)
    val proportionMismatched = noMismatches.toDouble / noCompounds
    val proportionMissing = noMissing.toDouble / noCompounds
    logger.warn(s"N compounds $noCompounds no mismatches $noMismatches Proportion of mismatches $proportionMismatched no missing $noMissing proportion missing $proportionMissing")
    if (proportionMismatched > 0.05) {
      val msg = s"excessive proportion [${proportionMismatched}] of compounds fail to match"
      logger.error(msg)
      throw new RuntimeException(msg)
    }
    if (proportionMissing > 0.01) {
      val msg = s"excessive proportion [${proportionMissing}] of compounds missing"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    require(rdkitDt.columns(0).title == "no")
    val dt = new DataTable(cdkDt.columns ++ rdkitDt.columns.tail)
    dt
  }

}