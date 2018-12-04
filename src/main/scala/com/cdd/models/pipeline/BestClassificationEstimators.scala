package com.cdd.models.pipeline

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.estimator.{SmileNaiveBayesClassifier, SmileSvmClassifier, SmileSvmRegressor, WekaRandomForestClassifier}
import com.cdd.models.pipeline.transformer.{FeatureScaler, FilterSameFeatureValues, VectorFolder}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.utils.HasLogging

import scala.collection.mutable.ArrayBuffer

object BestClassificationEstimators {

  val estimators: Vector[Pipeline] = createEstimators()

  // put this in after identifying the bug that may have resulted in datasets containing NaN features for rdkit descriptors.
  // Didn't seem to make much difference for Tox21 at least. Otherwise a full validation is required.
  private val useRdkitDescriptors = false

  /*
  Summary of best classifiers:

  RF does not work well on fingerprints

  SVM RBF appears suited to descriptors
  SVM Poly appears suited to fingerprints

   */
  private def createEstimators(): Vector[Pipeline] = {
    val estimators = ArrayBuffer[Pipeline]()


    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("cdk_desc")
      val rf = new WekaRandomForestClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setMaxDepth(15)
        .setNumTrees(70)
      new Pipeline().setStages(Vector(filter, rf))
        .setEstimatorName("Weka RF classification")
    }

    if (useRdkitDescriptors) {
      estimators += {
        val filter = new FilterSameFeatureValues()
          .setFeaturesColumn("rdkit_desc")
        val rf = new WekaRandomForestClassifier()
          .setFeaturesColumn("uniqueFeatures")
          .setMaxDepth(15)
          .setNumTrees(70)
        new Pipeline().setStages(Vector(filter, rf))
          .setEstimatorName("Weka RF classification on Rdkit descriptors")
      }
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_fp")
      val svm = new SmileSvmClassifier()
        .setKernelType("TANIMOTO")
        .setGamma(1.0)
        .setFeaturesColumn("uniqueFeatures")
        .setCoef0(1.0)
        .setDegree(2)
        .setC(1.0)
      new Pipeline().setStages(Vector(filter, svm))
        .setEstimatorName("Smile SVC Tanimoto classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_fp")
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
        .setFeaturesColumn("cdk_desc")
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

    if (useRdkitDescriptors) {
      estimators += {
        val filter = new FilterSameFeatureValues()
          .setFeaturesColumn("rdkit_desc")
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
          .setEstimatorName("Smile SVC RBF classification on rdkit descriptors")
      }
    }

    // this one is not so good, but wanted to include RF on fingerprints
    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("cdk_fp")
      val folder = new VectorFolder()
        .setFeaturesColumn("uniqueFeatures")
        .setFoldSize(1000)
      val rf = new WekaRandomForestClassifier()
        .setFeaturesColumn("foldedFeatures")
        .setMaxDepth(20)
        .setNumTrees(70)
      new Pipeline().setStages(Vector(filter, folder, rf))
        .setEstimatorName("Weka RF on folded fp classification")
    }

    estimators += {
      val filter = new FilterSameFeatureValues()
        .setFeaturesColumn("rdkit_fp")
      val nb = new SmileNaiveBayesClassifier()
        .setFeaturesColumn("uniqueFeatures")
        .setMethod("Bernoulli")
        .setAlpha(0.5)
      new Pipeline().setStages(Vector(filter, nb)).setEstimatorName("Smile Bernoulli NB classification")
    }

    // Bayes for larger systems and reference

    // TODO logistic regression and LSVC for large systems

    estimators.toVector
  }


}


case class NamedClassifierValidation(name: String, classifierValidation: ClassifierValidation)

@SerialVersionUID(1000L)
class ClassifierValidation(val label: String, val category: String, val estimator: Estimator[_,_],
                   val metric: Double, val dataSplit: DataSplit, val evaluator: ClassificationEvaluator) extends EstimatorValidation with Serializable{
}

class BestClassificationEstimators(val numFolds: Int = 3, val evaluator: ClassificationEvaluator = new ClassificationEvaluator()) extends HasLogging {

  def fitDataTable(dataLabel: String, dataCategory: String, dataTable: DataTable,
                   estimators:Vector[Pipeline]=BestClassificationEstimators.estimators): Vector[ClassifierValidation] = {
    estimators.map(runEstimator(dataLabel, dataCategory, _, dataTable)).sortBy(-_.metric)
  }


  def fitDataTableWithLabelTransforms[A, T <: Transformer with HasOutputLabelColumn]
  (labelTransform: T, estimator: Pipeline,
   parameter: Parameter[A],
   valuesAndDataCategories: Vector[(A, String)],
   dt: DataTable): Vector[ClassifierValidation] = {
    require(labelTransform.getParameter(labelTransform.labelColumn) == "inputLabel")
    require(labelTransform.getParameter(labelTransform.outputLabelColumn) == "label")
    valuesAndDataCategories.map { case (value, dataCategory) =>
      labelTransform.setParameter(parameter, value)
      val tdt = labelTransform.transform(dt)
      runEstimator("label", dataCategory, estimator, tdt)
    }
  }

  private def runEstimator(dataLabel: String, dataCategory: String, estimator: Pipeline,
                           dt: DataTable): ClassifierValidation = {
    logger.info(s"Evaluating classifier ${estimator.getEstimatorName()}")

    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(estimator).setNumFolds(numFolds)
    dataSplit.foldAndFit(dt)
    new ClassifierValidation(dataLabel, dataCategory, estimator,
      evaluator.evaluate(dataSplit.getPredictDt()), dataSplit, evaluator.copy(new ParameterMap()))
  }

}
