package com.cdd.models.vault

import java.io.{File, FileFilter, FileWriter}

import com.cdd.models.bae.AssayDirection
import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator.ClassificationStats
import com.cdd.models.pipeline.estimator.{OptimizeClassificationPredictions, RegressionToClassification}
import com.cdd.models.pipeline.{ClassificationEvaluator, ClassifierValidation, RegressionEvaluator, RegressorValidation}
import com.cdd.models.pipeline.tuning.IdenticalSplitLabels
import com.cdd.models.universalmetric.ClassificationPointType
import com.cdd.models.utils.Util.CartesianProductTooLargeException
import com.cdd.models.utils.{HasLogging, Util}
import com.cdd.models.vault.VaultDataTable.{ClassificationMethod, ClassificationMethodType}
import com.cdd.models.vault.VaultRegressionModel.DataTableTooLargeForRegression
import com.opencsv.CSVWriter
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.commons.math3.exception.OutOfRangeException

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

trait ProtocolFilesFinders extends HasLogging {
  val skipProtocols = Vector("v_1_d_KINASE: ChEMBL Kinase SARfari Compounds & BioAssay Data_p_Functional Data.obj",
    "v_1_d_KINASE: ChEMBL Kinase SARfari Compounds & BioAssay Data_p_ADMET Data.obj")

  def findProtocolObjects(patterns: Seq[String] = List("*PDSP*.obj", "*SARfari*.obj", "*PKIS*.obj")): Vector[String] = {
    patterns.flatMap(p => protocolFiles(p)).distinct.toVector
  }

  private def protocolFiles(pattern: String): Vector[String] = {
    val modelDirectory = Util.getProjectFilePath("data/vault/protocol_molecule_features/")
    val fileFilter = new WildcardFileFilter(pattern).asInstanceOf[FileFilter]
    modelDirectory
      .listFiles(fileFilter)
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .filter(_.endsWith(".obj"))
      .filter(f => !skipProtocols.exists(s => f.endsWith(s))).toVector
  }

  def datasetInformationAndDataTables(protocolFileName: String): (File, DatasetInformation, Vector[VaultDataTable]) = {
    val datasetInformationOpt = VaultReadouts.datasetInformationForObjectFile(protocolFileName)
    datasetInformationOpt match {
      case Some(information) =>
        logger.info(s"Loading information and tables for dataset id ${information.dataset.id} [${information.dataset.name}] protocol id ${information.protocol.id} [${information.protocol.name}")
      case None => throw new IllegalArgumentException(s"No dataset information for object file $protocolFileName")
    }
    val protocolReadouts = Util.deserializeObject[ProtocolFeaturesReadouts](protocolFileName)

    val datasetInformation = protocolReadouts.datasetInformation
    require(datasetInformation.dataset.id == datasetInformationOpt.get.dataset.id)
    require(datasetInformation.protocol.id == datasetInformationOpt.get.protocol.id)
    val protocolResultsDir = datasetInformation.resultsDir
    if (!protocolResultsDir.exists())
      require(protocolResultsDir.mkdir())

    val vaultDataTablesFile = new File(protocolResultsDir, "vaultDataTables.obj")
    val vaultDataTables = Util.withSerializedFile(vaultDataTablesFile) { () =>
      val allProtocolFeaturesReadouts =
        protocolReadouts.toGroupedData().filter(_.protocolReadouts.length > 100)
      val vaultDataTables = protocolReadouts.toDataTables(allProtocolFeaturesReadouts)
        .filter(_.dataTable.length > 100)
      vaultDataTables
    }
    if (vaultDataTables.nonEmpty)
      assert(vaultDataTables.groupBy(_.informationMd5()).mapValues(_.length).values.max == 1)

    (protocolResultsDir, datasetInformation, vaultDataTables)
  }

}


object PublicVaultEstimatorApp extends App with ProtocolFilesFinders with HasLogging {

  import PublicVaultEstimator._

  val files = if (args.length > 0) findProtocolObjects(args) else findProtocolObjects()

  Util.using(new FileWriter(regressionSummaryFile)) { regressionFh =>
    Util.using(new FileWriter(classificationSummaryFile)) { classificationFh =>

      Util.using(new FileWriter(regressionDetailFile)) { regressionDetailFh =>
        Util.using(new FileWriter(classificationDetailFile)) { classificationDetailFh =>

          val regressionSummaryWriter = new CSVWriter(regressionFh)
          regressionSummaryWriter.writeNext(regressionHeaders)
          val classificationSummaryWriter = new CSVWriter(classificationFh)
          classificationSummaryWriter.writeNext(classificationHeaders)

          val regressionDetailWriter = new CSVWriter(regressionDetailFh)
          regressionDetailWriter.writeNext(regressionHeaders)
          val classificationDetailWriter = new CSVWriter(classificationDetailFh)
          classificationDetailWriter.writeNext(classificationHeaders)

          for (file <- files) {
            runOnProtocol(file, Vector(regressionSummaryWriter), Vector(classificationSummaryWriter),
              Vector(regressionDetailWriter), Vector(classificationDetailWriter))
          }
        }
      }
    }
  }
}

object PublicVaultEstimator extends ProtocolFilesFinders with HasLogging {

  val regressionSummaryFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/current_regression.csv")
  val classificationSummaryFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/current_classification.csv")
  val regressionDetailFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/current_regression_detail.csv")
  val classificationDetailFile = Util.getProjectFilePath("data/vault/protocol_molecule_features/current_classification_detail.csv")

  val regressionHeaders = Array("DatasetId", "DatasetName", "ProtocolId", "ProtocolName", "LabelName", "GroupValues",
    "Size", "Estimator", "RMSE", "Correlation", "Spearman", "IC50", "MAE", "RegressionFile", "ProtocolFile")
  val classificationHeaders = Array("DatasetId", "DatasetName", "ProtocolId", "ProtocolName", "LabelName", "GroupValues",
    "Size", "Estimator", "ROC_AUC",
    "ClassificationPointType", "Accuracy", "Precision", "Recall", "f1", "Percent", "tp", "fp", "tn", "fn",
    "ClassificationFile", "ProtocolFile")
  val regressionToMultipleClassificationHeaders = Array("DatasetId", "DatasetName", "ProtocolId", "ProtocolName",
    "LabelName", "GroupValues", "Size", "Estimator", "ROC_AUC",
    "ClassificationPointType", "Direction", "Threshold", "Accuracy", "Precision", "Recall", "f1",
    "Percent", "tp", "fp", "tn", "fn", "correlation", "spearman", "regressionFile", "protocolFile"
  )

  def runOnProtocol(protocolFileName: String,
                    regressionSummaryWriters: Vector[CSVWriter], classificationSummaryWriters: Vector[CSVWriter],
                    regressionDetailWriters: Vector[CSVWriter], classificationDetailWriters: Vector[CSVWriter]
                   ): Unit = {

    val (protocolResultsDir, datasetInformation, vaultDataTables) = datasetInformationAndDataTables(protocolFileName)

    Util.using(new FileWriter(new File(protocolResultsDir, "classification.csv"))) { classificationFh =>
      Util.using(new FileWriter(new File(protocolResultsDir, "regression.csv"))) { regressionFh =>

        val cDetailWriters = classificationDetailWriters :+ new CSVWriter(classificationFh)
        val rDetailWriters = regressionDetailWriters :+ new CSVWriter(regressionFh)

        vaultDataTables.foreach { vdt =>
          val vaultEstimator = new PublicVaultEstimator(datasetInformation, vdt)
          vaultEstimator.writeVaultDataTableInformation()

          vaultEstimator.runRegressionOnVaultDataTable(regressionSummaryWriters, rDetailWriters, protocolFileName)
          vaultEstimator.runClassificationOnVaultDataTable(classificationSummaryWriters, cDetailWriters, protocolFileName)
        }
      }
    }
  }

  @SerialVersionUID(1000L)
  case class ClassifierFromRegressor(score: Double, dataTable: DataTable, regressorValidation: RegressorValidation,
                                     labelThreshold: Double, greaterBetter: Boolean, predictionThreshold: Double)
    extends Serializable

}

class PublicVaultEstimator(val datasetInformation: DatasetInformation,
                           val vdt: VaultDataTable) extends HasLogging {
  import PublicVaultEstimator._

  val protocolResultsDir = datasetInformation.resultsDir

  val protocolName = datasetInformation.protocol.name
  val labelName = vdt.labelReadout
  val groupValues = vdt.groupNamesString
  val vaultPrefix = vdt.informationMd5()

  def writeVaultDataTableInformation() = {

    val commonTerms = vdt.commonTerms
    val isIc50 = vdt.ic50Information.isDefined

    Util.using(new FileWriter(new File(protocolResultsDir, s"${vaultPrefix}_info.txt"))) { fh =>
      fh.write(s"Protocol: $protocolName\n")
      fh.write(s"Label Name: $labelName\n")
      fh.write(s"Group Values: $groupValues\n")
      fh.write(s"Common Terms: $commonTerms\n")
      fh.write(s"Ic50 Data: $isIc50\n")
    }

  }

  def regressionResultsFile = new File(protocolResultsDir, s"${vaultPrefix}_regression.obj")


  def runRegressionOnVaultDataTable(summaryWriters: Vector[CSVWriter], detailWriters: Vector[CSVWriter], protocolFileName: String):
  Option[(RegressionTable, Vector[RegressorValidation])] = {
    logger.info(s"Performing regression on protocol $protocolName label $labelName group names and values $groupValues data table size ${vdt.dataTable.length}")

    try {
      val (regressionDt, regressionResults) = Util.withSerializedFile(regressionResultsFile) { () =>
        vdt.applyRegressors()
      }
      assert(regressionDt.dataTable.length == vdt.dataTable.length)
      val correlationsAndFields = ArrayBuffer[(Double, Array[String])]()
      var evaluator: RegressionEvaluator = null

      regressionResults.foreach { r =>
        val predictDt = r.dataSplit.getPredictDt()
        evaluator = r.evaluator
        val mae = evaluator.mae(predictDt)
        val spearman = evaluator.spearmansCorrelation(predictDt)
        val fields = Array(
          datasetInformation.dataset.id.toString,
          datasetInformation.dataset.name,
          datasetInformation.protocol.id.toString,
          protocolName,
          labelName,
          groupValues,
          vdt.dataTable.length.toString,
          r.estimator.getEstimatorName(),
          r.metric.toString,
          r.correlation.toString,
          spearman.toString,
          if (vdt.ic50Information.isDefined) "yes" else "no",
          mae.toString,
          regressionResultsFile.getAbsolutePath,
          protocolFileName
        )
        detailWriters.foreach(_.writeNext(fields))
        correlationsAndFields.append((r.correlation, fields))
      }

      //val bestMetricAndField = metricsAndFields.maxBy(b => if (evaluator.isLargerBetter())  b._1 else -b._1)
      val bestMetricAndField = correlationsAndFields.maxBy(_._1)
      assert(bestMetricAndField._1 == regressionResults(0).correlation)
      summaryWriters.foreach(_.writeNext(bestMetricAndField._2))
      Some((regressionDt, regressionResults))
    }
    catch {
      case ex: IdenticalSplitLabels =>
        logger.error("Unable to create data splits", ex)
        None
      case ex: CartesianProductTooLargeException =>
        logger.error("Unable to perform cartesian product of readouts", ex)
        None
      case ex: DataTableTooLargeForRegression =>
        logger.error(ex.getMessage, ex)
        None
    }
  }


  def runClassificationOnVaultDataTable(summaryWriters: Vector[CSVWriter], detailWriters: Vector[CSVWriter], protocolFileName: String):
  Option[(ClassificationTable, Vector[ClassifierValidation])] = {
    try {
      val classificationMethod =
        new ClassificationMethod(ClassificationMethodType.FixedPercentage, 33.0)
      // new ClassificationMethod(ClassificationMethodType.FromHistogram, 33.0)
      val classificationResultsFile = new File(protocolResultsDir, s"${vaultPrefix}_classification_$classificationMethod.obj")
      val (classificationDt, classificationResults) = Util.withSerializedFile(classificationResultsFile) { () =>
        vdt.applyClassifiers(classificationMethod)
      }
      assert(classificationDt.dataTable.length == vdt.dataTable.length)
      val metricsAndFields = ArrayBuffer[(Double, Array[String])]()
      var evaluator: ClassificationEvaluator = null

      classificationResults.foreach { r =>
        val predictDt = r.dataSplit.getPredictDt()
        evaluator = r.evaluator
        val classificationStats = evaluator.statistics(predictDt)
        val precision = classificationStats.precision
        val recall = classificationStats.recall
        val accuracy = r.evaluator.accuracy(predictDt)
        val fields = Array(
          datasetInformation.dataset.id.toString,
          datasetInformation.dataset.name,
          datasetInformation.protocol.id.toString,
          protocolName, labelName, groupValues,
          classificationDt.dataTable.length.toString,
          r.estimator.getEstimatorName(), r.metric.toString, classificationDt.classificationPointType.toString,
          classificationDt.assayDirection.toString,
          classificationDt.threshold.toString, accuracy.toString, precision.toString, recall.toString,
          classificationStats.f1.toString, classificationMethod.percentage.toString, classificationStats.tp.toString,
          classificationStats.fp.toString, classificationStats.tn.toString, classificationStats.fn.toString,
          classificationResultsFile.getAbsolutePath,
          protocolFileName)
        detailWriters.foreach(_.writeNext(fields))
        metricsAndFields.append((r.metric, fields))
      }
      val bestMetricAndField = metricsAndFields.maxBy(b => if (evaluator.isLargerBetter()) b._1 else -b._1)
      assert(bestMetricAndField._1 == classificationResults(0).metric)
      summaryWriters.foreach(_.writeNext(bestMetricAndField._2))
      Some(classificationDt, classificationResults)
    }
    catch {
      case ex: IdenticalSplitLabels =>
        logger.error("Unable to create data splits", ex)
        None
      case ex: CartesianProductTooLargeException =>
        logger.error("Unable to perform cartesian product of readouts", ex)
        None
    }
  }

  private val defaultRegressionToClassificationEvaluator = (stats: ClassificationStats, rocAuc: Double) => {
    val np = stats.tp + stats.fn
    val length = np + stats.fp + stats.tn
    val percent = (np.toDouble * 100.0) / length.toDouble
    if (np < 10)
      None
    else if (percent < 5.0)
      None
    else if (stats.f1 < 0.5)
      None
    else
      Some(rocAuc)
  }

  private val defaultOptimizeOutputClassificationMetricFunction = (stats: ClassificationStats) => stats.f1

  def regressionToMultipleClassifiers(summaryWriters: Vector[CSVWriter], detailWriters: Vector[CSVWriter], protocolRefName: String,
                                      regressionResultsOption: Option[Vector[RegressorValidation]] = None,
                                      regressionToClassificationEvaluator: (ClassificationStats, Double) => Option[Double] = defaultRegressionToClassificationEvaluator,
                                      optimizeOutputClassificationMetricFunction: ClassificationStats => Double = defaultOptimizeOutputClassificationMetricFunction
                                     ): Vector[ClassifierFromRegressor] = {
    val regressionResults = regressionResultsOption match {
      case Some(results) => results
      case None =>
        val (_, results) = Util.deserializeObject[(RegressionTable, Vector[RegressorValidation])](regressionResultsFile.getAbsolutePath)
        results
    }

    val classificationThresholds = Vector.range(1, 50, 1).map { percent =>
      new ClassificationMethod(ClassificationMethodType.FixedPercentage, percent.toDouble)
    }
    val optimizeClassificationPredictions = new OptimizeClassificationPredictions()

    val evaluator = new ClassificationEvaluator()
    val regressionEvaluator = new RegressionEvaluator()
    val scoresAndFields = ArrayBuffer[(Double, Array[String])]()
    logger.info(s"Performing regression to classification on protocol $protocolName label $labelName group names and values $groupValues data table size ${vdt.dataTable.length}")

    val resultsByRegressor = regressionResults.flatMap { regressionResult =>

      val regressionPredictDt = regressionResult.dataSplit.getPredictDt()
      val correlation = regressionEvaluator.correlation(regressionPredictDt)
      require(regressionResult.correlation == correlation)
      val spearman = regressionEvaluator.spearmansCorrelation(regressionPredictDt)

      val classificationResults = classificationThresholds.flatMap { classificationThreshold =>
        logger.debug(s"Performing regression to classification on protocol $protocolName label $labelName group names and values $groupValues data table size ${vdt.dataTable.length} percent ${classificationThreshold.percentage}")

        // Create a classification data table from the regression result at this threshold (for labelling)
        val tryDataTableOpt = Try(performRegressionToClassification(regressionResult, classificationThreshold, vdt))
        val dataTableOpt = tryDataTableOpt match {
          case Success(v) => v
          case Failure(ex: IdenticalSplitLabels) =>
            logger.warn("Failed to partition data: no positive labels", ex)
            None
          case Failure(ex: OutOfRangeException) =>
            logger.warn("Failed to partition data: percentile out of range", ex)
            None
          case Failure(ex: Throwable) => throw ex
        }

        dataTableOpt.flatMap { results =>
          // we have a classification based on the regression- now tweak the threshold for prediction
          val (labelThreshold, greaterBetter, dataTableIn) = results
          val optimizeResults = optimizeClassificationPredictions.optimizeClassificationPrediction(dataTableIn, optimizeOutputClassificationMetricFunction)
          val dataTable = optimizeResults.dataTable

          val classificationStats = evaluator.statistics(dataTable)
          assert(classificationStats.fn + classificationStats.tp > 0)
          val rocAuc = evaluator.rocAuc(dataTable)
          if (rocAuc.isNaN)
            print(logger.error("roc AUC is NAN!"))
          require(!rocAuc.isNaN)

          // rank and filter this classification table
          val score = regressionToClassificationEvaluator(classificationStats, rocAuc)

          val accuracy = evaluator.accuracy(dataTable)
          val fields = Array(
            datasetInformation.dataset.id.toString,
            datasetInformation.dataset.name,
            datasetInformation.protocol.id.toString,
            protocolName, labelName, groupValues,
            dataTable.length.toString,
            regressionResult.estimator.getEstimatorName(), rocAuc.toString, ClassificationPointType.Fixed.toString,
            if (greaterBetter) "UP" else "DOWN", labelThreshold.toString,
            accuracy.toString, classificationStats.precision.toString, classificationStats.recall.toString,
            classificationStats.f1.toString, classificationThreshold.percentage.toString, classificationStats.tp.toString,
            classificationStats.fp.toString, classificationStats.tn.toString, classificationStats.fn.toString,
            correlation.toString, spearman.toString, regressionResultsFile.getAbsolutePath, protocolRefName)
          detailWriters.foreach(_.writeNext(fields))

          score.foreach(s => scoresAndFields.append((s, fields)))
          score.map(s => ClassifierFromRegressor(s, dataTable, regressionResult, labelThreshold, greaterBetter, optimizeResults.bestCutoff))
        }
      }

      if (classificationResults.nonEmpty)
        Some(classificationResults.maxBy(_.score))
      else
        None
    }.sortBy(-_.score)

    if (scoresAndFields.nonEmpty) {
      val bestScoreAndFields = scoresAndFields.maxBy(_._1)
      assert(bestScoreAndFields._1 == resultsByRegressor(0).score)
      summaryWriters.foreach(_.writeNext(bestScoreAndFields._2))
    }

    resultsByRegressor
  }


  private def performRegressionToClassification(regressionResult: RegressorValidation, classificationThreshold: ClassificationMethod, vdt: VaultDataTable) = {
    // Use the classification data table generation code to determine threshold and assay direction
    val cdt = vdt.toClassificationTable(classificationThreshold)
    val direction = cdt.assayDirection
    val greaterBetter = direction == AssayDirection.UP || direction == AssayDirection.UP_EQUAL

    val regressionPredictDt = regressionResult.dataSplit.getPredictDt()
    val regressionToClassification = new RegressionToClassification()
    regressionToClassification.toClassification(regressionPredictDt,
      cdt.threshold, greaterBetter,
      minPositiveCount = 1, minPositiveProportion = 0.0)
      .map {
        dataTable => (cdt.threshold, greaterBetter, dataTable)
      }
  }

  /**
    * Given regression results for a vault data table performs classification sampling.
    *
    * @param summaryWriters
    * @param protocolRefName
    * @param regressionResultsOption
    */
  def regressionToMultipleClassifiersOld(summaryWriters: Vector[CSVWriter], protocolRefName: String, regressionResultsOption: Option[Vector[RegressorValidation]] = None) = {
    val regressionResults = regressionResultsOption match {
      case Some(results) => results
      case None =>
        val (_, results) = Util.deserializeObject[(RegressionTable, Vector[RegressorValidation])](regressionResultsFile.getAbsolutePath)
        results
    }

    val classificationMethods = Vector.range(0, 50, 1).map { percent =>
      new ClassificationMethod(ClassificationMethodType.FixedPercentage, percent.toDouble)
    }
    val optCalculator = (stats: ClassificationStats) => stats.f1
    val optimizeClassificationPredictions = new OptimizeClassificationPredictions()

    val evaluator = new ClassificationEvaluator()
    val regressionEvaluator = new RegressionEvaluator()
    val regressionToClassification = new RegressionToClassification()

    for (regressionResult <- regressionResults) {

      val regressionPredictDt = regressionResult.dataSplit.getPredictDt()
      val correlation = regressionEvaluator.correlation(regressionPredictDt)
      require(regressionResult.correlation == correlation)
      val spearman = regressionEvaluator.spearmansCorrelation(regressionPredictDt)

      for (classificationMethod <- classificationMethods) {

        try {
          logger.info(s"Performing estimation on protocol $protocolName label $labelName group names and values $groupValues data table size ${vdt.dataTable.length} percent ${classificationMethod.percentage}")

          val cdt = vdt.toClassificationTable(classificationMethod)
          val direction = cdt.assayDirection
          val greaterBetter = direction == AssayDirection.UP || direction == AssayDirection.UP_EQUAL
          val dataTableOpt = regressionToClassification.toClassification(regressionPredictDt,
            cdt.threshold, greaterBetter,
            minPositiveCount = 1, minPositiveProportion = 0.0)
          if (dataTableOpt.isDefined) {
            val dataTableIn = dataTableOpt.get

            val optimizeResuts = optimizeClassificationPredictions.optimizeClassificationPrediction(dataTableIn, optCalculator)
            val dataTable = optimizeResuts.dataTable

            val classificationStats = evaluator.statistics(dataTable)
            assert(classificationStats.fn + classificationStats.tp > 0)
            val rocAuc = evaluator.rocAuc(dataTable)
            val accuracy = evaluator.accuracy(dataTable)

            val fields = Array(protocolName, labelName, groupValues,
              dataTable.length.toString,
              regressionResult.estimator.getEstimatorName(), rocAuc.toString, ClassificationPointType.Fixed.toString,
              direction.toString,
              cdt.threshold.toString, accuracy.toString,
              classificationStats.precision.toString, classificationStats.recall.toString,
              classificationStats.f1.toString, classificationMethod.percentage.toString, classificationStats.tp.toString,
              classificationStats.fp.toString, classificationStats.tn.toString, classificationStats.fn.toString,
              correlation.toString, spearman.toString, regressionResultsFile.getAbsolutePath, protocolRefName)
            summaryWriters.foreach(_.writeNext(fields))
          }
        }
        catch {
          case ex: IdenticalSplitLabels => logger.warn("Failed to partition data: no positive labels", ex)
          case ex: OutOfRangeException => logger.warn("Failed to partition data: percentile out of range", ex)
        }
      }
    }

  }

}

