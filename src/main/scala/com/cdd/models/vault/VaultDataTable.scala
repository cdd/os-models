package com.cdd.models.vault

import com.cdd.models.bae.AssayDirection
import com.cdd.models.bae.AssayDirection.AssayDirection
import com.cdd.models.bae.AssayNameMatcher.matcher
import com.cdd.models.datatable.DataTableColumn.NoneValuesPresentException
import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline.{ClassifierValidation, RegressorValidation}
import com.cdd.models.universalmetric.ClassificationPointType.ClassificationPointType
import com.cdd.models.universalmetric.{ClassificationPointType, DistributionType, HistogramDataType, NormalizedHistogram}
import com.cdd.models.utils.HasLogging
import com.cdd.models.vault.VaultDataTable.{ClassificationMethod, ClassificationMethodType}
import org.apache.commons.codec.digest.DigestUtils

object Ic50Unit extends Enumeration {

  protected case class Val(log: Boolean, exp: Double, reverse: Boolean = true) extends super.Val {

  }

  implicit def valueToIc50UnitVal(x: Value): Val = x.asInstanceOf[Val]

  val Picomolar = Val(false, -12.0)
  val Nanomolar = Val(false, -9.0)
  val Micromolar = Val(false, -6.0)
  val Millimolar = Val(false, -3.0)
  val pIc50 = Val(true, 0.0, false)
  val LogIc50 = Val(true, 0.0)

  val picomolarMatchers = Vector(
    matcher("""pM""".r),
    matcher("""(?i)pico *molar""".r)
  )

  val nanomolarMatchers = Vector(
    matcher("""nM""".r),
    matcher("""(?i)nano *molar""".r)
  )

  val micromolarMatchers = Vector(
    matcher("""uM""".r),
    matcher("""(?i)micro *molar""".r),
    matcher("um")
  )

  val millimolarMatchers = Vector(
    matcher("""mM""".r),
    matcher("""(?i)milli *molar""".r)
  )

  def stringToIc50Unit(str: String): Option[Ic50Unit.Val] = {
    if (picomolarMatchers.exists(_.matchName(str)))
      Some(Picomolar)
    else if (nanomolarMatchers.exists(_.matchName(str)))
      Some(Nanomolar)
    else if (micromolarMatchers.exists(_.matchName(str)))
      Some(Micromolar)
    else if (millimolarMatchers.exists(_.matchName(str)))
      Some(Millimolar)
    else
      None
  }
}

object VaultDataTable {
  val classificationMatchers = Vector(
    matcher("""(?i)inhibition""".r),
    matcher units ("%"),
    matcher units ("""(?i)percent""".r)
  )

  def isPrimaryReadout(readout: ReadoutDefinition): Boolean = {

    val units = readout.unitLabel match {
      case s: String => Some(s)
      case null => None
    }

    classificationMatchers.exists(_.matchName(readout.name, units))
  }

  object ClassificationMethodType extends Enumeration {
    type ClassificationMethodType = Value
    val FromHistogram, FixedPercentage = Value
  }

  import ClassificationMethodType._

  class ClassificationMethod(val classificationMethodType: ClassificationMethodType, val percentage: Double = 33.0) {
    override def toString: String = s"${classificationMethodType}_$percentage"
  }

}

class VaultIc50Information(val ic50Units: Ic50Unit.Value, val ic50DataTable: DataTable)

@SerialVersionUID(1000L)
class VaultDataTable(datasetInformation: DatasetInformation, val labelReadout: String,
                     val groupReadoutNamesAndValues: Option[Vector[(String, String)]],
                     val commonTerms: Map[String, Any],
                     val dataTable: DataTable) extends Serializable with HasLogging {

  @transient
  lazy val ic50Information: Option[VaultIc50Information] = findIc50Information()

  def toClassificationTable(classificationMethod: ClassificationMethod): ClassificationTable = {
    val isIc50 = ic50Information.isDefined

    val (dataTable, columnName, data) = if (isIc50) {
      val dt = ic50Information.get.ic50DataTable
      val cn = "p_ic50"
      (dt, cn, dt.column(cn).toDoubles())
    } else {
      val dt = this.dataTable
      (dt, "label", dt.column("label").toDoubles())
    }

    val (threshold, assayDirection, pointType) =
      if (classificationMethod.classificationMethodType == ClassificationMethodType.FromHistogram) {
        val histogram = toHistogram()
        val classificationPoint = histogram.findClassificationPoint(normalize = true)
        (classificationPoint.x, classificationPoint.assayDirection, classificationPoint.pointType)
      } else {
        val distribution = new DistributionType(data)
        val fixedDirection = if (isIc50) {
          Some(AssayDirection.UP)
        } else {
          None
        }
        val (thres, dir) = distribution.toActiveCategory(percentActive = classificationMethod.percentage, direction = fixedDirection)
        (thres, dir, ClassificationPointType.Fixed)
      }

    var greaterBetter = if (assayDirection == AssayDirection.UP) true else false
    val activeTable = dataTable.continuousColumnToDiscrete(columnName, threshold, greaterBetter)
    new ClassificationTable(assayDirection, threshold, activeTable, pointType)
  }

  private def findIc50Information(): Option[VaultIc50Information] = {
    val ic50Units = findIc50Units()
    if (ic50Units.isEmpty)
      return None

    try {
      val ic50DataTable = dataTableWithPic50Column(ic50Units.get)
      Some(new VaultIc50Information(ic50Units.get, ic50DataTable))
    } catch {
      case ex: NoneValuesPresentException =>
        logger.warn("Failed to create Ic50 datatable: zero or negative labels present")
        None
    }
  }

  private def findIc50Units(): Option[Ic50Unit.Value] = {

    val readoutDefinition = labelReadoutDefinition()
    if (readoutDefinition.unitLabel != null) {
      val unitLabel = readoutDefinition.unitLabel
      val ic50Unit = Ic50Unit.stringToIc50Unit(unitLabel)
      if (ic50Unit.isDefined)
        return ic50Unit
      return unitLabel match {
        case "%" => None
        case "ug/mL" => None
        case _ => throw new NotImplementedError(s"Unknown unit $unitLabel")
      }
    }

    val allTerms = groupReadoutNamesAndValues match {
      case Some(v) =>
        Map(v: _*) ++ commonTerms
      case None =>
        commonTerms
    }
    allTerms.collectFirst {
      case (k, v) if k.toLowerCase().contains("unit") && v.isInstanceOf[String] &&
        Ic50Unit.stringToIc50Unit(v.asInstanceOf[String]).isDefined =>
        Ic50Unit.stringToIc50Unit(v.asInstanceOf[String]).get
    }
  }

  def toRegressionTable(): RegressionTable = {
    ic50Information match {
      case Some(i) => new RegressionTable("p_ic50", i.ic50DataTable)
      case None => new RegressionTable("label", dataTable)
    }
  }

  def toHistogram(): NormalizedHistogram = {
    val rt = toRegressionTable()
    val data = rt.dataTable.column(rt.labelName).toDoubles()
    val name = rt.labelName match {
      case "p_ic50" => "PIC50"
      case "label" => "RAW"
      case _ => throw new IllegalArgumentException
    }
    val dataType = name match {
      case "PIC50" => HistogramDataType.PIC50
      case "RAW" => HistogramDataType.RAW
      case _ => throw new IllegalArgumentException
    }
    val title = s"${datasetInformation.dataset.name} ${datasetInformation.protocol.name} $name"
    new NormalizedHistogram(data, dataType = dataType, title = title)
  }

  def applyRegressors(): (RegressionTable, Vector[RegressorValidation]) = {
    logger.info(s"Performing regression on vault data table protocol ${datasetInformation.protocol.name} label $labelReadout group names and values $groupNamesString data table size ${dataTable.length}")
    val regressionTable = toRegressionTable()
    val regression = new VaultRegressionModel(regressionTable.dataTable)
    val regressionResults = regression.applyRegressors(dataLabel = regressionTable.labelName)
    (regressionTable, regressionResults)
  }

  def applyClassifiers(classificationMethod: ClassificationMethod =
                       new ClassificationMethod(ClassificationMethodType.FixedPercentage))
  : (ClassificationTable, Vector[ClassifierValidation]) = {
    logger.info(s"Performing classification on vault data table protocol ${datasetInformation.protocol.name} label ${labelReadout} group names and values ${groupNamesString} data table size ${dataTable.length}")
    val classificationTable = toClassificationTable(classificationMethod)
    val classification = new VaultClassificationModel(classificationTable.dataTable)
    val classificationResults = classification.applyClassifiers("active")
    (classificationTable, classificationResults)
  }

  private def dataTableWithPic50Column(ic50Unit: Ic50Unit.Value): DataTable = {

    val columnName =
      if (ic50Unit.log) {
        "label"
      } else {
        "log_label"
      }
    val values = dataTable.column(columnName).toDoubles()

    val pIc50Values = values.map { v =>
      val newValue = v + ic50Unit.exp
      if (ic50Unit.reverse)
        -newValue
      else
        newValue
    }

    dataTable.addColumn(DataTableColumn.fromVector("p_ic50", pIc50Values))
  }

  private def labelReadoutDefinition(): ReadoutDefinition = {

    val readoutOpt = datasetInformation.protocol.readoutDefinitions.find(_.name == labelReadout)
    assert(readoutOpt.isDefined)
    readoutOpt.get
  }

  private def isPrimary(): Boolean = {
    val readout = labelReadoutDefinition()
    VaultDataTable.isPrimaryReadout(readout)
  }

  def groupNamesString: String = {
    groupReadoutNamesAndValues match {
      case Some(readoutNamesAndValues) =>
        readoutNamesAndValues.map(t => s"${t._1}:${t._2}").mkString("|")
      case None =>
        "NA"
    }
  }

  def commonTermsString: String = {
    commonTerms.map { case (k, v) => s"$k:${v.toString}" }.mkString("|")
  }

  def informationMd5(): String = {
    val datasetString = s"${datasetInformation.vaultId.toString}:${datasetInformation.protocol.id}:${datasetInformation.protocol.name}"
    val informationString = datasetString + labelReadout + groupNamesString + commonTermsString
    val digest = DigestUtils.md5Hex(informationString)
    logger.info(s"informationMd5: info is $informationString digest is $digest")
    digest
  }

  def information: String = {
    s"${datasetInformation.information} label $labelReadout groupNames $groupNamesString common terms $commonTermsString"
  }
}

@SerialVersionUID(1000L)
class RegressionTable(val labelName: String, val dataTable: DataTable) extends Serializable

@SerialVersionUID(1000L)
class ClassificationTable(val assayDirection: AssayDirection, val threshold: Double, val dataTable: DataTable,
                          val classificationPointType: ClassificationPointType) extends Serializable
