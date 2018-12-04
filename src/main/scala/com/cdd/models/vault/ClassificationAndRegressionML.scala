package com.cdd.models.vault

import java.io.FileWriter

import com.cdd.models.datatable.{DataTable, DataTableColumn, DataTableModelUtil}
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.estimator.{MajorityVotingClassifier, SmileSvmClassifier, WekaRandomForestClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVWriter

import scala.collection.mutable.ArrayBuffer

object ClassificationAndRegressionMLApp extends App with HasLogging {
  ClassificationAndRegressionML.fitDataTable(true)
}

object ClassificationAndRegressionVotingMLApp extends App with HasLogging {
  ClassificationAndRegressionML.fitDataTableWithVotingClassifier()
}

object BuildAndSaveModelsOnAllData extends App {
  ClassificationAndRegressionML.buildAndSaveModelsOnAllData()
}

object ClassificationAndRegressionML extends HasLogging {

  val featureNames = Vector(
    "RocAUC",
    "Accuracy",
    "Precision",
    "Recall",
    "Correlation",
    "Spearman",
    "Cost",
    "Kurtosis",
    "Skew"
  )

  def savedModelFileName(modelName:String): String = {
      Util.getProjectFilePath(s"data/vault/protocol_molecule_features/classification_regression_ml_models/$modelName.model")
        .getAbsolutePath
  }

  def buildDataTable(bothIsRegression: Boolean = true): DataTable = {
    val dataTableIn = DataTable.loadProjectFile(DatasetSummary.summaryFileWithCategories.getAbsolutePath)
    //.shuffle()
    val dataTableWithFeatures = DataTableModelUtil.addFeatureColumn(dataTableIn, featureNames)
    val badDataTable = dataTableWithFeatures.filterByColumnCriteria[Double]("RocAUC", v => v <= 0.6)
    badDataTable.rows.foreach { r =>
      val row = r.map { case(k, v) => (k, v.get) }
      val name = s"${row("ProtocolName")} ${row("LabelName")} ${row("GroupValues")}"
      val test = row("Category").asInstanceOf[String]
      test match {
        case "Regression" => logger.warn(s"row $name (rocAuc ${row("RocAUC")}) is set Regression")
        case "Classification" => logger.warn(s"row $name (rocAuc ${row("RocAUC")}) is set Classification")
        case "Both" =>logger.warn(s"row $name (rocAuc ${row("RocAUC")}) is set Both")
        case _ =>
      }
     // require(test == "Bad")
    }

    val dataTableBadRemoved = dataTableWithFeatures.filterByColumnCriteria[Double]("RocAUC", v => v > 0.6)
    dataTableBadRemoved.rows.foreach { r =>
      val row = r.map { case(k, v) => (k, v.get) }
      val name = s"${row("ProtocolName")} ${row("LabelName")} ${row("GroupValues")}"
      row("Category").asInstanceOf[String] match {
        case "Bad" => logger.warn(s"row $name (rocAuc ${row("RocAUC")}) is set Bad")
        case "Unassigned" => logger.warn(s" row $name is not set")
        case _ =>
      }

    }

    addLabelsColumn(dataTableBadRemoved, bothIsRegression = bothIsRegression)
  }

  private val evaluator = new ClassificationEvaluator().setMetric("f1")

  def buildAndSaveModelsOnAllData(): Vector[PipelineModel] = {
    val classifiers = createOptimizedEstimators() :+ buildVotingClassifierOnOptimziedClassifiers()
    val dataTable = buildDataTable(true)
    val models = classifiers.map { classifier =>
      val model = classifier.fit(dataTable)
      val modelFileName = savedModelFileName(classifier.getEstimatorName())
      logger.info(s"Saving to $modelFileName")
      model.savePipeline(modelFileName)

      model
    }

    models
  }

  private def buildVotingClassifierOnOptimziedClassifiers(): Pipeline = {
    val classifiers = createOptimizedEstimators()
    val votingClassifier = new MajorityVotingClassifier()
    votingClassifier.setClassifiers(classifiers)
    votingClassifier.tieIsPositive(false)
    val pipeline = new Pipeline().setStages(Vector(votingClassifier)).setEstimatorName("Voting Classifier")
    pipeline
  }

  def fitDataTableWithVotingClassifier(): Unit = {
    val votingClassifier = buildVotingClassifierOnOptimziedClassifiers()
    val dataTable = buildDataTable(true)

    val evaluator = new ClassificationEvaluator().setMetric("f1")

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(votingClassifier).setNumFolds(3)
    dataSplit.foldAndFit(dataTable)
    val predictDt = dataSplit.getPredictDt()
    val f1 = evaluator.evaluate(predictDt)
    val stats = evaluator.statistics(predictDt)
    val accuracy = evaluator.accuracy(predictDt)
    logger.info(s"f1 $f1 Accuracy $accuracy TP ${stats.tp} FP ${stats.fp} TN ${stats.tn} FN ${stats.fn} Recall ${stats.recall} Precision ${stats.precision}")
  }

  def fitDataTable(optimizedEstimators: Boolean = true): Unit = {

    val bestEstimators = new BestClassificationEstimators(evaluator = evaluator)
    val estimators = if (optimizedEstimators) createOptimizedEstimators() else createEstimators()
    val dataTable = buildDataTable()

    val results = bestEstimators.fitDataTable("RegressionFit", "BothIsRegression", dataTable,
      estimators = estimators)

    val results2 = bestEstimators.fitDataTable("RegressionFit", "BothIsClassification", dataTable,
      estimators = estimators)

    val fileName = if (optimizedEstimators) "opt_classification_and_regression_ml.csv" else "classification_and_regression_ml.csv"
    val resultsFile = Util.getProjectFilePath(s"data/vault/protocol_molecule_features/$fileName")
    Util.using(new FileWriter(resultsFile)) { fh =>
      val csvWriter = new CSVWriter(fh)

      var fields = Array("Estimator", "f1", "ROC_AUC", "Accuracy", "TP", "FP", "TN", "FN", "Recall", "Precision", "Category")
      csvWriter.writeNext(fields)
      writeResults(csvWriter, results)
      writeResults(csvWriter, results2)
    }
  }

  def writeResults(csvWriter: CSVWriter, results: Vector[ClassifierValidation]) = {
    results.foreach { result =>
      val dt = result.dataSplit.getPredictDt()
      val stats = evaluator.statistics(dt)
      val rocAuc = evaluator.rocAuc(dt)
      val accuracy = evaluator.accuracy(dt)
      val fields = Array(result.estimator.getEstimatorName(), result.metric.toString, rocAuc.toString, accuracy.toString,
        stats.tp.toString, stats.fp.toString,
        stats.tn.toString, stats.fn.toString, stats.recall.toString, stats.precision.toString, result.category)
      csvWriter.writeNext(fields)
    }

  }

  def addLabelsColumn(dt: DataTable, bothIsRegression: Boolean = true) = {
    val labels = dt.column("Category").values.map { valueOpt =>
      require(valueOpt.isDefined)
      valueOpt.get match {
        case "Classification" => 0.0
        case "Both" => if (bothIsRegression) 1.0 else 0.0
        case "Regression" => 1.0
        case _ => throw new IllegalArgumentException(s"Unknown value ${valueOpt.get}")
      }
    }
    dt.addColumn(DataTableColumn.fromVector("label", labels))
  }

  private def createEstimators(): Vector[Pipeline] = {
    val estimators = ArrayBuffer[Pipeline]()


    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val rf = new WekaRandomForestClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setMaxDepth(15)
        .setNumTrees(70)
      new Pipeline().setStages(Vector(filter, rf))
        .setEstimatorName("Weka RF classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val rf = new WekaRandomForestClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setMaxDepth(80)
        .setNumTrees(100)
      new Pipeline().setStages(Vector(filter, rf))
        .setEstimatorName("Weka RF classification 2")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmClassifier()
        .setKernelType("POLY")
        .setGamma(1.0)
        .setFeaturesColumn("normalizedFeatures")
        .setCoef0(6.0)
        .setDegree(2)
        .setC(0.0001)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Smile SVC Poly classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmClassifier()
        .setKernelType("RBF")
        .setFeaturesColumn("normalizedFeatures")
        .setGamma(1.0)
        .setC(1.0)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Smile SVC RBF classification")
    }


    estimators.toVector
  }

  def createOptimizedEstimators(): Vector[Pipeline] = {
    val estimators = ArrayBuffer[Pipeline]()


    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val rf = new WekaRandomForestClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setMaxDepth(5)
        .setNumTrees(80)
      new Pipeline().setStages(Vector(filter, rf))
        .setEstimatorName("Weka RF classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmClassifier()
        .setKernelType("POLY")
        .setGamma(1.0)
        .setFeaturesColumn("normalizedFeatures")
        .setCoef0(4.0)
        .setDegree(3)
        .setC(1.0)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Smile SVC Poly classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmClassifier()
        .setKernelType("RBF")
        .setFeaturesColumn("normalizedFeatures")
        .setGamma(0.1)
        .setC(10.0)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Smile SVC RBF classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("features")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("Standard")
      val svr = new SmileSvmClassifier()
        .setKernelType("Linear")
        .setFeaturesColumn("normalizedFeatures")
        .setC(10.0)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Smile SVC Linear classification")
    }

    estimators.toVector
  }


}

