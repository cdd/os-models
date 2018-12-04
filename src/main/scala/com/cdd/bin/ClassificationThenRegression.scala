package com.cdd.bin

import com.cdd.models.datatable.{ActivityTransform, SdfileReader}
import com.cdd.models.pipeline.estimator.{SmileRandomForestClassifier, SmileRandomForestRegressor}
import com.cdd.models.pipeline.transformer.{FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.pipeline.{ClassificationEvaluator, Pipeline, RegressionEvaluator}
import com.cdd.models.utils.{ActivityCategory, SdfileActivityField, Util}

object ClassificationThenRegression extends App {

  // example arguments: -sdfile TrypsinI-Bostaurus-SP-B-CHEMBL3769-Binding-C50.sdf.gz -transform nM_to_pIc50 -field Value -threshold 0 -greaterBetter true
  // run in <os-models>/data/OSMD test data
  if (args.length == 0) {
    print(s"Usage: ${ClassificationThenRegression.getClass} -sdfile <file> [-transform <transform>] -field <field> [-imageFile <imageFile>] -threshold threshold -greaterBetter <true|false>")
    System.exit(1)
  }
  def optionsMap(map: Map[Symbol, String], args: List[String]): Map[Symbol, String] = {
    args match {
      case Nil => map
      case "-sdfile" :: value :: tail => optionsMap(map ++ Map('sdfile -> value), tail)
      case "-transform" :: value :: tail => optionsMap(map ++ Map('transform -> value), tail)
      case "-field" :: value :: tail => optionsMap(map ++ Map('field -> value), tail)
      case "-imageFile" :: value :: tail => optionsMap(map ++ Map('imageFile -> value), tail)
      case "-threshold" :: value :: tail => optionsMap(map ++ Map('threshold -> value), tail)
      case "-greaterBetter" :: value :: tail => optionsMap(map ++ Map('greaterBetter -> value), tail)
      case option :: tail => throw new IllegalArgumentException(s"Unknown option ${option}")
    }
  }

  val options = optionsMap(Map(), args.toList)
  val fileName = options('sdfile)
  val field = options('field)
  val threshold= options('threshold).toDouble
  val transform = options.get('transform).map(t=>ActivityTransform.stringToTransform(t).get)
  val imageFileName = options.get('imageFileName)
  val greaterBetter = options('greaterBetter).equalsIgnoreCase("true")

  val activityField = SdfileActivityField(field, ActivityCategory.CONTINUOUS, transform)
  val classificationThenRegression =
    ClassificationThenRegressionHelper.classificationThenRegression(fileName, activityField, threshold,
      greaterBetter, imageFileName)

}

object ClassificationThenRegressionHelper {

  def classificationThenRegression(fileName: String, property: SdfileActivityField, threshold: Double,
                                   greaterBetter: Boolean, imageFileNameOption:Option[String]=None):Unit = {
    var dt = SdfileReader.sdFileToRdkitDataTable(fileName, Vector(property))
    dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp"))
      .filterNull().shuffle()
    dt = dt.continuousColumnToDiscrete("label", threshold, greaterBetter = true, newColumnName = "active")

    val pipeline = new Pipeline()
    val filter = new FilterSameFeatureValues()
      .setFeaturesColumn("rdkit_fp").setOutputColumn("uniqueFeatures")
    dt = filter.fit(dt).transform(dt)
    val folder = new VectorFolder()
      .setFoldSize(2000)
      .setOutputColumn("foldedFeatures")
      .setFeaturesColumn("uniqueFeatures")
    dt = folder.transform(dt)
    val rf = new SmileRandomForestClassifier()
      .setFeaturesColumn("foldedFeatures")
      .setNumTrees(80)
      .setMaxNodes(10000)
      .setLabelColumn("active")

    val classificationEvaluator = new ClassificationEvaluator()
      .setLabelColumn("active")
      .setMetric("rocAuc")
    val classificationDataSplit = new DataSplit()
      .setNumFolds(5)
      .setEvaluator(classificationEvaluator)
      .setEstimator(rf)
    classificationDataSplit.foldAndFit(dt)
    val classificationDt = classificationDataSplit.getPredictDt()
    val classificationChart = classificationEvaluator.chart(classificationDt)

    val regressionEvaluator = new RegressionEvaluator()
      .setLabelColumn("label")
      .setPredictionColumn("regression_prediction")
      .setMetric("rmse")
    val rfr = new SmileRandomForestRegressor()
      .setFeaturesColumn("foldedFeatures")
      .setNumTrees(80).setMaxNodes(10000)
      .setPredictionColumn("regression_prediction")
    val allRegressionDataSplit = new DataSplit()
      .setNumFolds(5)
      .setEvaluator(regressionEvaluator)
      .setEstimator(rfr)
    allRegressionDataSplit.foldAndFit(dt)
    val allRegressionDt = allRegressionDataSplit.getPredictDt()
    val allRegressionChart = regressionEvaluator.chart(allRegressionDt)

    val (_, activeDt) = classificationDt.classifierPredictionSplit("active")
    val (_, predictedActiveDt) = classificationDt.classifierPredictionSplit("prediction")

    val regressionDataSplit = new DataSplit()
      .setNumFolds(5)
      .setEvaluator(regressionEvaluator)
      .setEstimator(rfr)
    regressionDataSplit.foldAndFit(activeDt)
    var regressionDt = regressionDataSplit.getPredictDt()
    val regressionChart = regressionEvaluator.chart(regressionDt)

    val predictedModel = rfr.fit(activeDt)
    val predictedActiveRegressionDt = predictedModel.transform(predictedActiveDt)
    val regressionChart2 = regressionEvaluator.chart(predictedActiveRegressionDt)

    val imageFileName = imageFileNameOption.getOrElse("classification_and_regression.png")
   // Util.gridPlot(imageFileName, Vector(allRegressionChart, classificationChart, regressionChart, regressionChart2), cols=2)
    Util.gridPlot(imageFileName, Vector(allRegressionChart, classificationChart, regressionChart), cols=3)
  }
}
