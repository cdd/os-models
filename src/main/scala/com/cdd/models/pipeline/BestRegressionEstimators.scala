package com.cdd.models.pipeline

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.{LinearRegressor, SmileSvmRegressor, SvmRegressor, WekaRandomForestRegressor}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.utils.HasLogging
import org.apache.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

object BestRegressionEstimators {

  val estimators: Vector[Pipeline] = createEstimators()

  /*
  Summary of best estimators:

  RF does not work well on fingerprints

  SVM RBF appears suited to descriptors
  SVM Poly appears suited to fingerprints

   */
  private def createEstimators(): Vector[Pipeline] = {
    val estimators = ArrayBuffer[Pipeline]()


    estimators += {
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp")
      val svm = new SmileSvmRegressor().setKernelType("TANIMOTO")
        .setGamma(1.0)
        .setFeaturesColumn("uniqueFeatures")
        .setC(1.0)
        .setCoef0(3.0)
        .setDegree(2)
        .setMaxIter(2000)
      new Pipeline().setStages(Vector(filter, svm))
        .setEstimatorName("Tanimoto Smile SVM regression")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_desc")
      val rf = new WekaRandomForestRegressor()
        .setFeaturesColumn("uniqueFeatures")
        .setMaxDepth(20)
        .setNumTrees(50)
      new Pipeline().setStages(Vector(filter, rf))
        .setEstimatorName("Weka RF descriptors regression")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_desc")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmRegressor()
        .setKernelType("RBF")
        .setFeaturesColumn("normalizedFeatures")
        .setMaxIter(2000)
        .setGamma(0.1)
        .setC(10)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("RBF Smile SVM descriptors regression")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_fp")
      val folder = new VectorFolder()
        .setFeaturesColumn("uniqueFeatures")
        .setFoldSize(2000)
      val scaler = new FeatureScaler()
        .setFeaturesColumn("foldedFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SvmRegressor()
        .setKernelType("RBF")
        .setFeaturesColumn("normalizedFeatures")
        .setGamma(0.01)
        .setC(10.0)
      new Pipeline().setStages(Vector(filter, folder, scaler, svr))
        .setEstimatorName("RBF libSVM folded fingerprints regression")
    }

    estimators += {
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp")
      val scaler = new FeatureScaler()
        .setFeaturesColumn("uniqueFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val svr = new SmileSvmRegressor()
        .setKernelType("POLY")
        .setGamma(1.0)
        .setFeaturesColumn("normalizedFeatures")
        .setMaxIter(2000)
        .setDegree(2)
        .setCoef0(6.0)
        .setC(0.1)
      new Pipeline().setStages(Vector(filter, scaler, svr))
        .setEstimatorName("Poly Smile SVM fingerprints regression")
    }

    estimators += {
      // for Elastic Net non-folded fingerprints perform slightly better, but this should be more scalable
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp")
      val folder = new VectorFolder()
        .setFeaturesColumn("uniqueFeatures")
        .setFoldSize(2000)
      val scaler = new FeatureScaler()
        .setFeaturesColumn("foldedFeatures")
        .setOutputColumn("normalizedFeatures")
        .setScalerType("MaxMin")
      val lr = new LinearRegressor()
        .setFeaturesColumn("normalizedFeatures")
        .setElasticNetParam(0.25)
        .setMaxIter(100)
        .setLambda(0.05)
      new Pipeline()
        .setStages(Vector(filter, folder, scaler, lr))
        .setEstimatorName("Elastic Net on folded fingerprints regression")
    }

    estimators += {
      val filter = new FilterSameFeatureValues().setFeaturesColumn("cdk_fp")
      val folder = new VectorFolder().setFeaturesColumn("uniqueFeatures").setFoldSize(500)
      val rf = new WekaRandomForestRegressor().setFeaturesColumn("foldedFeatures").setNumTrees(70).setMaxDepth(50)
      new Pipeline().setStages(Vector(filter, folder, rf))
        .setEstimatorName("Weka RF folded fingerprints regression")
    }

    estimators.toVector
  }
}

trait EstimatorValidation {
  val label: String
  val category: String
  val estimator: Estimator[_, _]
  val metric: Double
  val dataSplit: DataSplit
  val evaluator: Evaluator
}

case class NamedEstimatorValidation(name: String, estimatorValidation: EstimatorValidation)

@SerialVersionUID(1000L)
class RegressorValidation(val label: String, val category: String, val estimator: Pipeline,
                          val metric: Double, val correlation: Double, val dataSplit: DataSplit,
                          val evaluator: RegressionEvaluator) extends EstimatorValidation with Serializable

case class NamedRegressorValidation(name: String, regressorValidation: RegressorValidation)

class BestRegressionEstimators(val numFolds: Int = 3, val evaluator: RegressionEvaluator = new RegressionEvaluator()) extends HasLogging {

  def fitDataTable(dataLabel: String, dataCategory: String, dataTable: DataTable): Vector[RegressorValidation] = {
    BestRegressionEstimators.estimators.map(runEstimator(dataLabel, dataCategory, _, dataTable)).sortBy(-_.correlation)
  }

  def fitDataTableWithLabelTransforms[A, T <: Transformer with HasOutputLabelColumn]
  (labelTransform: T, estimator: Pipeline,
   parameter: Parameter[A],
   valuesAndDataCategories: Vector[(A, String)],
   dt: DataTable): Vector[RegressorValidation] = {
    require(labelTransform.getParameter(labelTransform.labelColumn) == "inputLabel")
    require(labelTransform.getParameter(labelTransform.outputLabelColumn) == "label")
    valuesAndDataCategories.map { case (value, dataCategory) =>
      labelTransform.setParameter(parameter, value)
      val tdt = labelTransform.transform(dt)
      runEstimator("label", dataCategory, estimator, tdt)
    }
  }

  private def runEstimator(dataLabel: String, dataCategory: String, estimator: Pipeline,
                           dt: DataTable): RegressorValidation = {
    logger.info(s"Evaluating regressor ${estimator.getEstimatorName()}")
    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(estimator).setNumFolds(numFolds)
    dataSplit.foldAndFit(dt)
    new RegressorValidation(dataLabel, dataCategory, estimator, dataSplit.getMetric(),
      evaluator.correlation(dataSplit.getPredictDt()), dataSplit, evaluator.copy(new ParameterMap()))
  }


}
