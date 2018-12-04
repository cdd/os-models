package com.cdd.models.pipeline

import java.io.File

import com.cdd.models.datatable.DataTableModelUtil
import com.cdd.models.tox.{AzAmdeData, Tox21Compounds}
import com.cdd.models.utils.Util
import org.scalatest.FunSpec
import org.scalatest.Matchers

class SerializedPipelineSpec1 extends FunSpec with Matchers {

  val modelDirectory = Util.getProjectFilePath("data/saved_models")

  describe("Comparing the results of an in-memory pipeline with the saved pipeline for best classification models") {

    val tox212DataTable = Tox21Compounds.buildDf()
    val dataTable = DataTableModelUtil.selectTable(tox212DataTable, "NR-AR")

    val (trainData, testData) = dataTable.testTrainSplit()
    val evaluator = new ClassificationEvaluator().setMetric("rocAuc")
    val classifiers = BestClassificationEstimators.estimators

    classifiers.foreach { classifier =>
      val name = classifier.getEstimatorName
      describe(s"Checking pipeline based on classifier $name") {

        val model = classifier.fit(trainData)
        val inMemoryPrediction = model.transform(testData)
        val inMemoryMetric = evaluator.evaluate(inMemoryPrediction)

        val file = new File(modelDirectory, s"tox21_AR_AR_${name.replace(' ', '_')}_spec.model")
        val fileName = file.getAbsolutePath
        if (file.exists())
          file.delete()

        model.savePipeline(fileName)

        val savedModel = PipelineModel.loadPipeline(fileName)
        val savedModelPrediction = savedModel.transform(testData)
        val saveModelFile = fileName.replace(".model", ".csv.gz")
        savedModelPrediction.exportToFile(saveModelFile)
        val savedModelMetric = evaluator.evaluate(savedModelPrediction)
        val testSmiles = new TestSmiles(fileName)

        info(s"classifier model $name has metric $inMemoryMetric\n")

        it(s"should have the same rocAUC for the in memory and saved model") {
          inMemoryMetric should be(savedModelMetric +- 0.0001)
        }

        val smiles = if (model.getFeaturesColumn().startsWith("rdkit"))
          testData.column("rdkit_smiles").toStrings()
        else
          testData.column("cdk_smiles").toStrings()
        val (_, mP) = evaluator.labelsAndPredictions(inMemoryPrediction)
        val (_, sP) = evaluator.labelsAndPredictions(savedModelPrediction)
        mP.zip(sP).zip(smiles).zipWithIndex.foreach { case (((m, s), smi), i) =>
          val r = testSmiles.predict(Vector(smi))
          it(s"should have the same predictions for the in memory and saved model for item $i") {
            m should be(s +- 0.000001)
            m should be(r(0).get +- .00001)
          }
        }

        val mProb = inMemoryPrediction.column("probability").toDoubles()
        val sProb = savedModelPrediction.column("probability").toDoubles()
        mProb.zip(sProb).zip(smiles).zipWithIndex.foreach { case (((m, s), smi), i) =>
          val r = testSmiles.predictWithProbabilities(Vector(smi))
          it(s"should have the same probability for the in memory and saved model for item $i") {
            m should be(s +- 0.000001)
            m should be(r(0).get._2 +- .00001)
          }

        }
      }
    }
  }


  describe("Comparing the results of an in-memory pipeline with the saved pipeline for best regression models") {

    val azAdmeData = new AzAmdeData
    val protocol = azAdmeData.protocols(0)
    val downloader = azAdmeData.datasets(0)
    val dataTable = DataTableModelUtil.selectTable(downloader.downloadFile())
    val (trainData, testData) = dataTable.testTrainSplit()
    val evaluator = new RegressionEvaluator().setMetric("rmse")
    val regressors = BestRegressionEstimators.estimators

    regressors.foreach {
      regressor =>
        val name = regressor.getEstimatorName
        describe(s"Checking pipeline based on regressor $name") {

          val model = regressor.fit(trainData)
          val inMemoryPrediction = model.transform(testData)
          val inMemoryMetric = evaluator.evaluate(inMemoryPrediction)

          val file = new File(modelDirectory, s"az_${
            downloader.readoutName.replace(' ', '_')
          }_${
            name.replace(' ', '_')
          }_spec.model")
          val fileName = file.getAbsolutePath
          if (file.exists())
            file.delete()

          model.savePipeline(fileName)

          val savedModel = PipelineModel.loadPipeline(fileName)
          val savedModelPrediction = savedModel.transform(testData)
          val saveModelFile = fileName.replace(".model", ".csv.gz")
          savedModelPrediction.exportToFile(saveModelFile)
          val savedModelMetric = evaluator.evaluate(savedModelPrediction)
          val testSmiles = new TestSmiles(fileName)

          info(s"regression model $name has metric $inMemoryMetric\n")

          it(s"should have the same rmse for the in memory and saved model") {
            inMemoryMetric should be(savedModelMetric +- 0.0001)
          }

          val (_, mP) = evaluator.labelsAndPredictions(inMemoryPrediction)
          val (_, sP) = evaluator.labelsAndPredictions(savedModelPrediction)
          val smiles = if (model.getFeaturesColumn().startsWith("rdkit"))
            testData.column("rdkit_smiles").toStrings()
          else
            testData.column("cdk_smiles").toStrings()
          mP.zip(sP).zip(smiles).zipWithIndex.foreach {
            case (((m, s), smi), i) =>
              val r = testSmiles.predict(Vector(smi))
              it(s"should have the same predictions for the in memory and saved model for item $i") {
                m should be(s +- 0.000001)
                m should be(r(0).get +- 0.000001)
              }
          }

        }
    }

  }

}
