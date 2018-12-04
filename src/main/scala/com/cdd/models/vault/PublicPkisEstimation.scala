package com.cdd.models.vault

import java.io.{BufferedReader, FileReader, FileWriter}

import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.models.vault.VaultDataTable.{ClassificationMethod, ClassificationMethodType}
import com.opencsv.CSVWriter

@Deprecated
object PublicPkisEstimation extends App with HasLogging {

  val base = "v_1_d_KINASE: GSK Published Kinase Inhibitor Set (PKIS)_p_Kinase Assay"
  val filePath = Util.getProjectFilePath("data/vault/protocol_molecule_features/" + base + ".obj")
  val protocolReadouts = Util.deserializeObject[ProtocolFeaturesReadouts](filePath.getAbsolutePath)
  val noMolecules = protocolReadouts.protocolReadouts.size
  val allProtocolFeaturesReadouts = protocolReadouts.toGroupedData().filter(_.protocolReadouts.length > 100)
  val vaultDataTables = protocolReadouts.toDataTables(allProtocolFeaturesReadouts)
  logger.info(s"Processing ${vaultDataTables.length} data tables")

  val classificationMethod = new ClassificationMethod(ClassificationMethodType.FixedPercentage, 0.33)
  val classificationDataTables = vaultDataTables.map {
    _.toClassificationTable(classificationMethod)
  }

  val classificationResultsFilePath = Util.getProjectFilePath("data/vault/protocol_molecule_features/classification_" + base + ".csv")
  val regressionResultsFilePath = Util.getProjectFilePath("data/vault/protocol_molecule_features/regression_" + base + ".csv")
  Util.using(new FileWriter(classificationResultsFilePath.getAbsoluteFile)) { classificationResultsFh =>
    Util.using(new FileWriter(regressionResultsFilePath.getAbsoluteFile)) { regressionResultsFh =>

      val regressionResultsWriter = new CSVWriter(regressionResultsFh)
      var fields = Array("GroupName", "GroupValue", "Size", "Estimator", "RMSE", "Correlation")
      regressionResultsWriter.writeNext(fields)

      val classificationResultsWriter = new CSVWriter(classificationResultsFh)
      fields = Array("GroupName", "GroupValue", "Size", "Estimator", "ROC_AUC", "Direction", "Threshold")
      classificationResultsWriter.writeNext(fields)

      vaultDataTables.zip(classificationDataTables).foreach { case (regressionDt, classificationDt) =>
        require(regressionDt.groupReadoutNamesAndValues.isDefined)
        require(regressionDt.groupReadoutNamesAndValues.get.size == 1)
        val groupReadoutName = regressionDt.groupReadoutNamesAndValues.get(0)._1
        val groupReadoutValue = regressionDt.groupReadoutNamesAndValues.get(0)._2

        logger.info(s"Processing ${groupReadoutName} value ${groupReadoutValue}")

        val classification = new VaultClassificationModel(classificationDt.dataTable)
        val classificationResults = classification.applyClassifiers("active")
        classificationResults.zipWithIndex.foreach { case (r, i) =>
          fields = Array(groupReadoutName, groupReadoutValue,
            classificationDt.dataTable.length.toString,
            r.estimator.getEstimatorName(), r.metric.toString, classificationDt.assayDirection.toString,
            classificationDt.threshold.toString)
          classificationResultsWriter.writeNext(fields)
        }

        val regression = new VaultRegressionModel(regressionDt.dataTable)
        val regressionResults = regression.applyRegressors()
        regressionResults.zipWithIndex.foreach { case (r, i) =>
          fields = Array(groupReadoutName, groupReadoutValue,
            regressionDt.dataTable.length.toString,
            r.estimator.getEstimatorName(), r.metric.toString, r.correlation.toString)
          regressionResultsWriter.writeNext(fields)
        }

      }
    }
  }
}

