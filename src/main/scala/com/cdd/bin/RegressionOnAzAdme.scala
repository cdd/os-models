package com.cdd.bin


import com.cdd.models.datatable.{ActivityTransform, DataTable, SdfileReader}
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline, RegressionEvaluator}
import com.cdd.models.pipeline.estimator.{SmileRandomForestClassifier, SmileRandomForestRegressor, SvmRegressor}
import com.cdd.models.pipeline.transformer.{ActivityFilter, FilterSameFeatureValues, StandardScaler, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.utils.{ActivityCategory, SdfileActivityField, Util}
import org.knowm.xchart.XYChart

import scala.collection.mutable.ArrayBuffer

class AzAdmeData {
  var dataTable: Option[DataTable] = None

  val fields = Vector(
    "AZ Dog protein binding: Protein binding (%)",
    "AZ human hepatocyte intrinsic clearance: Intrinsic clearance (uL.min-1.(10^6cells)-1)",
    "AZ Mouse protein binding: Protein binding (%)",
    "AZ Rat protein binding: Protein binding (%)")

  def readFiles: DataTable = {
    val sdfile = Util.getProjectFilePath("data/AZ_ADME.sdf.gz")
    val csvfile = Util.getProjectFilePath("data/AZ_ADME.csv.gz")

    //val transform = ActivityTransform.transformToLog10

    val properties =
      fields
        .map(new SdfileActivityField(_, ActivityCategory.CONTINUOUS, None))

    val dt = if (csvfile.exists()) {
      DataTable.loadFile(csvfile.getAbsolutePath)
    } else {
      SdfileReader.sdFileToRdkitDataTable(sdfile.getAbsolutePath, properties)
    }

    dataTable = Some(dt)
    dt
  }

  def classificationModelDataSplits(): Vector[DataSplit] = {
    fields.zipWithIndex.map { case (f, no) =>
      val columnName = if (no == 0) "activity_value" else s"activity_value_${no + 1}"
      val threshold = if (f.contains("human")) 5 else 80
      var fieldDt = dataTable.get
        .selectAs(("no" -> "no"), (columnName -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("rdkit_descriptors" -> "rdkit_desc"))
        .filterNull()
        .continuousColumnToDiscrete("label", threshold, false, "active").shuffle()
      println(s"Processing field $f number of rows ${fieldDt.length}")

      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_desc").setOutputColumn("uniqueFeatures")
      fieldDt = filter.fit(fieldDt).transform(fieldDt)
      val folder = new VectorFolder().setFoldSize(2000).setOutputColumn("foldedFeatures").setFeaturesColumn("uniqueFeatures")
      fieldDt = folder.transform(fieldDt)
      val rf = new SmileRandomForestClassifier().setFeaturesColumn("foldedFeatures").setNumTrees(80).setMaxNodes(10000).setLabelColumn("active")

      val evaluator = new ClassificationEvaluator().setLabelColumn("active")
      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(rf)
      dataSplit.foldAndFit(fieldDt)
      dataSplit.getPredictDt()
      dataSplit
    }
  }
}

object RegressionOnAzAdme extends App {

  val azAdmeData = new AzAdmeData
  val dt = azAdmeData.readFiles

  val results = azAdmeData.fields.zipWithIndex.foldLeft[Option[DataTable]](None) { case (results, (f, no)) =>
    val columnName = if (no == 0) "activity_value" else s"activity_value_${no + 1}"
    val fieldDt = dt.selectAs(("no" -> "no"), (columnName -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"), ("rdkit_descriptors" -> "rdkit_desc")).filterNull().shuffle()
    println(s"Processing field $f number of rows ${fieldDt.length}")

    val pipeline = new Pipeline()
    if (false) {
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp").setOutputColumn("uniqueFeatures")
      val svr = new SvmRegressor().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures").setC(1.0).setCoef0(2.0).setDegree(2)
      pipeline.setStages(Vector(filter, svr))
    } else if (false) {
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_desc").setOutputColumn("uniqueFeatures")
      val scaler = new StandardScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("scaledFeatures")
      val svr = new SvmRegressor().setKernelType("RBF").setGamma(1.0).setFeaturesColumn("scaledFeatures").setC(1.0).setGamma(0.01)
      pipeline.setStages(Vector(filter, scaler, svr))
    } else {

      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp").setOutputColumn("uniqueFeatures")
      val folder = new VectorFolder().setFoldSize(2000).setOutputColumn("foldedFeatures").setFeaturesColumn("uniqueFeatures")
      val rfr = new SmileRandomForestRegressor().setFeaturesColumn("foldedFeatures").setNumTrees(80).setMaxNodes(10000)
      pipeline.setStages(Vector(filter, folder, rfr))
    }

    val evaluator = new RegressionEvaluator()
    val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(pipeline)
    dataSplit.foldAndFit(fieldDt)
    var modelDt = dataSplit.getPredictDt()
    modelDt = modelDt.addConstantColumn("System", f)
    results match {
      case None => Some(modelDt)
      case Some(r) => Some(r.append(modelDt))
    }
  }

  val outFile = Util.getProjectFilePath("data/AZ_ADME_results.csv.gz").getCanonicalPath
  results.get.exportToFile(outFile)

}

object ClassificationOnAzAdme extends App {

  val azAdmeData = new AzAdmeData
  val dt = azAdmeData.readFiles

  val modelDataSplits = azAdmeData.classificationModelDataSplits()


  val results = azAdmeData.fields.zip(modelDataSplits).zipWithIndex.foldLeft[Option[DataTable]](None) { case (results, ((f, dataSplit), no)) =>

    var modelDt = dataSplit.getPredictDt()
    val roc = dataSplit.getParameter(dataSplit.evaluator).asInstanceOf[ClassificationEvaluator].rocAuc(modelDt)

    println(s"Processed field $f number of rows ${modelDt.length} data split aucROC $roc accuracy ${dataSplit.getMetric()}")
    modelDt = modelDt.addConstantColumn("System", f).select("System", "no", "label", "active", "prediction", "probability")
    results match {
      case None => Some(modelDt)
      case Some(r) => Some(r.append(modelDt))
    }
  }

  val outFile = Util.getProjectFilePath("data/AZ_ADME_classification_results.csv.gz").getCanonicalPath
  results.get.exportToFile(outFile)
}


object ClassificationThenRegressionOnAzAdme extends App {

  val azAdmeData = new AzAdmeData
  val dt = azAdmeData.readFiles
  val charts = ArrayBuffer[XYChart]()

  val results = azAdmeData.fields.zip(azAdmeData.classificationModelDataSplits()).zipWithIndex.foldLeft[Option[DataTable]](None) {
    case (results, ((f, classificationDataSplit), no)) =>
      val dataTable = classificationDataSplit.getPredictDt()
      val classiferEvaluator = classificationDataSplit.getParameter(classificationDataSplit.evaluator).asInstanceOf[ClassificationEvaluator]
      charts += classiferEvaluator.chart(dataTable)

      val (_, dt) = dataTable.classifierPredictionSplit("active")
      val rfr = new SmileRandomForestRegressor().setFeaturesColumn("foldedFeatures").setNumTrees(80).setMaxNodes(10000).setPredictionColumn("regression_prediction")
      val evaluator = new RegressionEvaluator().setLabelColumn("label").setPredictionColumn("regression_prediction")
      val regressionDataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(rfr)
      regressionDataSplit.foldAndFit(dt)
      var modelDt = regressionDataSplit.getPredictDt()
      charts += evaluator.chart(modelDt)

      val (_, predictedActiveDt) = dataTable.classifierPredictionSplit("prediction")
      val predictedModel = rfr.fit(dt)
      val predictedActiveRegressionDt = predictedModel.transform(predictedActiveDt)
      //charts += evaluator.chart(predictedActiveRegressionDt)

      modelDt = modelDt.addConstantColumn("System", f).selectAs("System" -> "System", "no" -> "no", "label" -> "label", "regression_prediction" -> "prediction")
      results match {
        case None => Some(modelDt)
        case Some(r) => Some(r.append(modelDt))
      }
  }

  val imageFile = Util.getProjectFilePath("data/AZ_ADME_classification_and_regression_results.png").getCanonicalPath
  Util.gridPlot(imageFile, charts.toVector, cols = 2)

  val outFile = Util.getProjectFilePath("data/AZ_ADME_classification_then_regression_results.csv.gz").getCanonicalPath
  results.get.exportToFile(outFile)
}

object ProcessClassificationOnAzAdme extends App {
  val resultsFile = Util.getProjectFilePath("data/AZ_ADME_classification_results.csv.gz").getCanonicalPath
  val dt = DataTable.loadFile(resultsFile)
  val testSystems = dt.column("System").uniqueValues().asInstanceOf[Vector[String]]
  var (dataTables, titles) = testSystems.map { t =>
    val tdt = dt.filterByColumnValues("System", Vector(t))
    val title = t.take(30)
    (tdt, title)
  }.unzip


  val imageFile = Util.getProjectFilePath("data/AZ_ADME_classification_results.png").getCanonicalPath
  new ClassificationEvaluator().setMetric("rocAuc").setLabelColumn("active").gridPlot(imageFile, dataTables, titles, cols = 2)
}