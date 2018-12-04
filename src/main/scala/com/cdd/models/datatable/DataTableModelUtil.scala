package com.cdd.models.datatable

import com.cdd.models.pipeline.tuning.IdenticalSplitLabels
import com.cdd.models.universalmetric.DistributionType
import com.cdd.models.utils.HasLogging

object DataTableModelUtil extends HasLogging {

  def selectTable(dt: DataTable, labelName: String = "label"): DataTable = {
    var cols = Vector("no" -> "no",
      labelName -> "label",
      "fingerprints_RDKit_FCFP6" -> "rdkit_fp",
      "rdkit_descriptors" -> "rdkit_desc",
      "fingerprints_CDK_FCFP6" -> "cdk_fp",
      "cdk_descriptors" -> "cdk_desc",
      "rdkit_smiles" -> "rdkit_smiles",
      "cdk_smiles" -> "cdk_smiles")
    if (dt.hasColumnWithName("vault_desc")) {
      cols = cols :+ ("vault_desc" -> "vault_desc")
    }
    dt.selectAs(cols: _*).filterNull()
  }

  def toLogTable(dt: DataTable, labelName: String = "label", logLabelName: String = "label"): Option[DataTable] = {
    val distribution = new DistributionType(dt.column(labelName).asInstanceOf[DataTableColumn[Double]].values.flatten)
    if (distribution.toLogDistribution().isDefined) {
      val logValues = dt.column(labelName).asInstanceOf[DataTableColumn[Double]].values.map {
        case Some(v) => Some(Math.log(v))
        case None => None
      }
      val logColumn = new DataTableColumn[Double](logLabelName, logValues)
      val newDt = dt.rename(labelName -> s"arithmetic_$labelName").addColumn(logColumn)
      Some(newDt)
    } else {
      None
    }
  }

  def addFeatureColumn(dt: DataTable, featureNames: Vector[String], featureColumnTitle: String = "features"): DataTable = {
    val featureVectors = Vector.range(0, dt.length).map { rowNo =>
      val values = dt.rowToMap(rowNo)
      val featureVector = featureNames.map { featureName =>
        if (!values.contains(featureName))
          logger.warn(s"Missing feature $featureName from input table")
        require(values.contains(featureName))
        val valueOpt = values(featureName)
        require(valueOpt.isDefined)
        valueOpt.get match {
          case v: String => v.toDouble
          case v: Int => v.toDouble
          case v: Double => v
          case _ => throw new IllegalArgumentException
        }
      }
      new DenseVector(featureVector)
    }
    dt.addColumn(DataTableColumn.fromVector(featureColumnTitle, featureVectors))
  }

}
