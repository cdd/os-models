package com.cdd.models.vault

import java.io.{File, FileFilter, FileWriter}

import com.cdd.models.pipeline.tuning.IdenticalSplitLabels
import com.cdd.models.utils.Util.CartesianProductTooLargeException
import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.models.vault.VaultDataTable.{ClassificationMethod, ClassificationMethodType}
import com.opencsv.CSVWriter
import org.apache.commons.io.filefilter.WildcardFileFilter

object PublicVaultMultipleClassificationCutoff extends App with ProtocolFilesFinders with HasLogging {

  val classificationHeaders = Array("ProtocolName", "LabelName", "GroupValues", "Size", "Estimator", "ROC_AUC",
    "ClassificationPointType", "Direction", "Threshold", "Accuracy", "Precision", "Recall", "f1", "Percent", "tp", "fp", "tn", "fn")

  val classificationSummaryFile = args(0)
  val files = if (args.length>1) findProtocolObjects(args.drop(1)) else findProtocolObjects()

  Util.using(new FileWriter(classificationSummaryFile)) { classificationFh =>

    val classificationSummeryWriter = new CSVWriter(classificationFh)
    classificationSummeryWriter.writeNext(classificationHeaders)

    for (file <- files) {
      runOnProtocol(file, Some(classificationSummeryWriter))
    }
  }

  def runOnProtocol(protocolFileName: String,
                    classificationSummaryWriter: Option[CSVWriter]) = {
    val (protocolResultsDir, datasetInformation, vaultDataTables) = datasetInformationAndDataTables(protocolFileName)
    Util.using(new FileWriter(new File(protocolResultsDir, "classification.csv"))) { classificationFh =>

      val classificationWriter = new CSVWriter(classificationFh)
      classificationWriter.writeNext(classificationHeaders)

      vaultDataTables.foreach { vdt =>

        val protocolName = datasetInformation.protocol.name
        val labelName = vdt.labelReadout
        val groupValues = vdt.groupNamesString
        val commonTerms = vdt.commonTerms
        val isIc50 = vdt.ic50Information.isDefined

        val vaultPrefix = vdt.informationMd5()
        Util.using(new FileWriter(new File(protocolResultsDir, s"${vaultPrefix}_info.txt"))) { fh =>
          fh.write(s"Protocol: $protocolName\n")
          fh.write(s"Label Name: $labelName\n")
          fh.write(s"Group Vaules: $groupValues\n")
          fh.write(s"Common Terms: $commonTerms\n")
          fh.write(s"Ic50 Data: $isIc50\n")
        }


        val classificationMethods = Vector(20, 30, 40, 50).map { percent =>
          new ClassificationMethod(ClassificationMethodType.FixedPercentage, percent)
        }

        for (classificationMethod <- classificationMethods) {
          try {
            logger.info(s"Performing estimation on protocol $protocolName label $labelName group names and values $groupValues data table size ${vdt.dataTable.length} percent ${classificationMethod.percentage}")

            val classificationResultsFile = new File(protocolResultsDir, s"classification_${vaultPrefix}_$classificationMethod.obj")
            val (classificationDt, classificationResults) = Util.withSerializedFile(classificationResultsFile) { () =>
              vdt.applyClassifiers(classificationMethod)
            }
            classificationResults.foreach { r =>
              val predictDt = r.dataSplit.getPredictDt()
              val classificationStats = r.evaluator.statistics(predictDt)
              val precision = classificationStats.precision
              val recall = classificationStats.recall
              val f1 = classificationStats.f1
              val accuracy = r.evaluator.accuracy(predictDt)
              val fields = Array(protocolName, labelName, groupValues,
                classificationDt.dataTable.length.toString,
                r.estimator.getEstimatorName(), r.metric.toString, classificationDt.classificationPointType.toString,
                classificationDt.assayDirection.toString,
                classificationDt.threshold.toString, accuracy.toString, precision.toString, recall.toString,
                f1.toString, classificationMethod.percentage.toString, classificationStats.tp.toString,
                classificationStats.fp.toString, classificationStats.tn.toString, classificationStats.fn.toString)
              classificationWriter.writeNext(fields)
              classificationSummaryWriter.foreach(_.writeNext(fields))
            }
          }
          catch {
            case ex: IdenticalSplitLabels =>
              logger.error("Unable to create data splits", ex)
            case ex: CartesianProductTooLargeException =>
              logger.error("Unable to perform cartesian product of readouts", ex)
          }
        }
      }
    }
  }
}