package com.cdd.models.vault

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter
import PublicVaultEstimator.{ClassifierFromRegressor, _}
import com.cdd.models.datatable.DenseVector
import com.cdd.models.pipeline._
import com.cdd.models.vault.DatasetCategory.DatasetCategory
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics

import scala.collection.mutable

object TryanosomeClassificationAndRegressionMLValidate extends App with HasLogging {

  import ClassificationAndRegressionMLValidate._

  loadDatasets("*TRYPANOSOME*.obj",
    Util.getProjectFilePath("data/vault/protocol_molecule_features/TrypanosomeValidation.txt"))
}

object TbClassificationAndRegressionMLValidate extends App with HasLogging {

  import ClassificationAndRegressionMLValidate._

  loadDatasets("*v_1_d_TB*.obj",
    Util.getProjectFilePath("data/vault/protocol_molecule_features/TbValidation.txt"))
}


object MalariaClassificationAndRegressionMLValidate extends App with HasLogging {

  import ClassificationAndRegressionMLValidate._

  loadDatasets("v_1_d_MALARIA*.obj",
    Util.getProjectFilePath("data/vault/protocol_molecule_features/MalariaValidation.txt"))
}

object ClassificationAndRegressionMLValidate extends HasLogging with ProtocolFilesFinders {

  def loadDatasets(pattern: String, summaryFile: File): Unit = {

    Util.using(new PrintWriter(new FileWriter(summaryFile))) { summaryFh =>

      summaryFh.println(s"Looking for protocol objects matching $pattern")
      val files = findProtocolObjects(List(pattern))
      files.zipWithIndex.foreach { case (file, fileNo) =>
        logger.info(s"Processing file $fileNo -  $file")
        summaryFh.println(s"processing object file $file")
        //if (fileNo == 1)
        processFile(file, summaryFh)
        logger.info(s"read file $file")
        summaryFh.println(s"done processing object file $file")
      }
    }
  }

  def processFile(fileName: String, summaryFh: PrintWriter): Unit = {

    val (protocolResultsDir, datasetInformation, vaultDataTables) = datasetInformationAndDataTables(fileName)
    summaryFh.println(s"processing in directory ${protocolResultsDir.getAbsolutePath} ${datasetInformation.information}")

    Util.using(new FileWriter(new File(protocolResultsDir, "regression.csv"))) { regressionFh =>
      val regressionWriter = new CSVWriter(regressionFh)
      regressionWriter.writeNext(regressionHeaders)

      Util.using(new FileWriter(new File(protocolResultsDir, "classification.csv"))) {
        classificationFh =>
          val classificationWriter = new CSVWriter(classificationFh)
          classificationWriter.writeNext(classificationHeaders)

          vaultDataTables.zipWithIndex.foreach { case (vdt, vdtNo) =>
            val vaultValidation = new VaultClassificationAndRegressionMLValidate(datasetInformation, vdt)
            vaultValidation.processVault(Vector(regressionWriter), Vector(classificationWriter), summaryFh)
          }

      }
    }
  }
}

object VaultClassificationAndRegressionMLValidateResult {
  val modelFileName: String = ClassificationAndRegressionML.savedModelFileName("Voting Classifier")
  val savedVotingModel: PipelineModel = PipelineModel.loadPipeline(modelFileName)
  val featureTester: TestFeatures = new TestFeatures(savedVotingModel)
}

@SerialVersionUID(1000L)
class VaultClassificationAndRegressionMLValidateResult(val datasetInformation: DatasetInformation, val vaultInformation: String,
                                                       val vaultPrefix: String, val regression: RegressorValidation,
                                                       val classification: ClassifierFromRegressor) extends  Serializable {

  import VaultClassificationAndRegressionMLValidateResult._

  def predict(): (DatasetCategory, Map[String, Double]) = {
    val regressionDatatable = regression.dataSplit.getPredictDt()
    val classificationDatatable = classification.dataTable

    val classificationEvaluator = new ClassificationEvaluator()
    val rocAuc = classificationEvaluator.rocAuc(classificationDatatable)
    val accuracy = classificationEvaluator.accuracy(classificationDatatable)
    val stats = classificationEvaluator.statistics(classificationDatatable)
    val cost = stats.scaledCost()

    val regressionEvaluator = new RegressionEvaluator()
    val correlation = regressionEvaluator.correlation(regressionDatatable)
    val spearman = regressionEvaluator.spearmansCorrelation(regressionDatatable)

    val inputValues = regressionDatatable.column("label").toDoubles()
    val distribution = new DescriptiveStatistics(inputValues.toArray)

    val featureMap = Map[String, Double](
      "RocAUC" -> rocAuc,
      "Accuracy" -> accuracy,
      "Precision" -> stats.precision,
      "Recall" -> stats.recall,
      "Correlation" -> correlation,
      "Spearman" -> spearman,
      "Cost" -> cost,
      "Kurtosis" -> distribution.getKurtosis,
      "Skew" -> distribution.getSkewness
    )

    val featureValues = ClassificationAndRegressionML.featureNames.map {
      featureMap(_)
    }

    val category = if (rocAuc < 0.6) {
      DatasetCategory.Bad
    } else {
      val features = new DenseVector(featureValues)
      val prediction = featureTester.predict(features)
      if (prediction == 1.0)
        DatasetCategory.Regression
      else
        DatasetCategory.Classification
    }

    (category, featureMap)

  }
}

object VaultClassificationAndRegressionMLValidate {
}

class VaultClassificationAndRegressionMLValidate(val datasetInformation: DatasetInformation, val vdt: VaultDataTable) extends HasLogging {

  import VaultClassificationAndRegressionMLValidate._

  def processVault(regressionWriters: Vector[CSVWriter], classificationWriters: Vector[CSVWriter],
                   summaryFh: PrintWriter): Unit = {
    val protocolResultsDir = datasetInformation.resultsDir
    val prefix = vdt.informationMd5()

    summaryFh.println(s"processing vault data table ${vdt.information}")
    Util.using(new FileWriter(new File(protocolResultsDir, s"${prefix}_regression.csv"))) { regressionFh =>

      val regressionWriter = new CSVWriter(regressionFh)
      regressionWriter.writeNext(regressionHeaders)

      Util.using(new FileWriter(new File(protocolResultsDir, s"${prefix}_classification.csv"))) {
        classificationFh =>
          val classificationSummaryWriter = new CSVWriter(classificationFh)
          classificationSummaryWriter.writeNext(classificationHeaders)

          val datasetName = datasetInformation.dataset.name
          val protocolName = datasetInformation.protocol.name

          logger.info(s"Processing vault dataset ${vdt.information}")
          val publicVaultEstimator = new PublicVaultEstimator(datasetInformation, vdt)
          val regressionResults = publicVaultEstimator.runRegressionOnVaultDataTable(Vector(), Vector(regressionWriter),
            datasetInformation.featuresFile.getAbsolutePath)

          val classificationResults =
            regressionResults match {
              case None => None
              case Some((regressionDt, regressionValidations)) =>
                assert(regressionDt.dataTable.length == vdt.dataTable.length)
                val classifiers = publicVaultEstimator.regressionToMultipleClassifiers(Vector(),
                  Vector(classificationSummaryWriter), protocolName, Some(regressionValidations))
                if (classifiers.nonEmpty)
                  Some(classifiers)
                else
                  None
            }

          if (classificationResults.isDefined && regressionResults.isDefined) {

            val bestRegressionValidation = regressionResults.get._2(0)
            val bestClassifierFromRegression = classificationResults.get(0)

            val resultFile = new File(protocolResultsDir, s"${prefix}_um_result.obj")
            val result = Util.withSerializedFile(resultFile) { () =>
            new VaultClassificationAndRegressionMLValidateResult(datasetInformation, vdt.information,
                vdt.informationMd5(), bestRegressionValidation, bestClassifierFromRegression)
            }
            val (category, featureMap) = result.predict()

            var msg = s"Feature Map ${featureMap.map(_.productIterator.mkString("->")).mkString("|")}"
            summaryFh.println(msg)
            logger.info(msg)

            Util.serializeObject(resultFile.getAbsolutePath, result)
            msg = s"Category: ${category.toString} Result File: '${resultFile.getAbsolutePath}'"
            summaryFh.println(msg)
            logger.info(msg)
          }
          else {
            if (regressionResults.isEmpty)
              summaryFh.println(s"No regression results for dataset!")
            if (classificationResults.isEmpty)
              summaryFh.println(s"No classification results for dataset!")
          }

          summaryFh.flush()
      }
    }
  }

}


