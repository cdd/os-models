package com.cdd.models.pipeline


import com.cdd.models.datatable._
import com.cdd.models.pipeline.estimator.{SmileUtil, WekaUtil}
import com.cdd.models.utils.{HasLogging, Modifier}
import org.apache.log4j.LogManager
import smile.math.SparseArray
import weka.core.Instances

abstract class ModelBase[M <: ModelBase[M, F, T], F, T] extends RowTransformer[F, T] with Serializable

abstract class Model[M <: Model[M, T], T] extends ModelBase[M, VectorBase, T]

abstract class EstimatorBase[M <: ModelBase[M, F, T], F, T] extends PipelineStage {
   val estimatorName = new Parameter[String](this, "estimatorName", "A user defined name/label for the estimator")

  def setEstimatorName(value: String): this.type = setParameter(estimatorName, value)

  setDefaultParameter(estimatorName, "Estimator")

  def getEstimatorName(): String = getParameter(estimatorName)

  /**
    * Fits a single model to the input data with provided parameter map.
    *
    */
  def fit(dataTable: DataTable, parameterMap: ParameterMap): M = {
    copy(parameterMap).fit(dataTable)
  }

  /**
    * Fits a model to the input data.
    */
  def fit(dataTable: DataTable): M

}

@SerialVersionUID(2374659848066372096L)
abstract class Estimator[M <: Model[M, T], T] extends EstimatorBase[M, VectorBase, T]


abstract class PredictionModel[M <: PredictionModel[M]] extends Model[M, Double] with HasPredictionParameters {

  override def transform(dataTable: DataTable): DataTable = {

    val allFeatures = dataTable.column(getParameter(featuresColumn)).toVector()
    val predictions = allFeatures.map {
      transformRow(_)
    }

    dataTable.addColumn(new DataTableColumn[Double](getParameter(predictionColumn), predictions.map(Some(_))))
  }

  def transformRow(features: VectorBase): Double

}

abstract class Predictor[M <: PredictionModel[M]] extends Estimator[M, Double] with HasPredictionParameters

abstract class FeatureMappingModel[M <: FeatureMappingModel[M]] extends Model[M, VectorBase] with FeatureMappingRowTransformer

abstract class FeatureMapperEstimator[M <: FeatureMappingModel[M]] extends Estimator[M, VectorBase] with HasOutputColumn


trait HasLabelColumn extends HasParameters {

  val labelColumn = new Parameter[String](this, "labelColumn", "The data table column containing labels")

  def setLabelColumn(value: String): this.type = setParameter(labelColumn, value)

  setDefaultParameter(labelColumn, "label")

  /**
    * As a measure of label dimensionality determine the mean of absolute label values.
    *
    * @param dataTable
    * @return
    */
  def labelDimensionality(dataTable: DataTable):Double = {
    val absValues = dataTable.column(getParameter(labelColumn)).toDoubles().map(Math.abs)
    absValues.sum/absValues.size.toDouble
  }

}

trait HasCensoredLabelColumn extends  HasParameters {
  val censoredLabelColumn = new Parameter[String](this, "censorLabelColumn", "The data table column containing censored labels")

  def setCensoredLabelColumn(value:String) : this.type = setParameter(censoredLabelColumn, value)

  setDefaultParameter(censoredLabelColumn, "modifier")

  def extractModifiers(dataTable: DataTable): Vector[Modifier] = {
    dataTable.column(getParameter(censoredLabelColumn)).toStrings().map(Modifier.fromString)
  }
}

trait HasFeaturesAndLabelParameters extends HasFeaturesParameters with HasLabelColumn with HasLogging{

  def extractFeaturesAndLabels(dataTable: DataTable): (Vector[Double], Vector[VectorBase]) = {
    val labels = dataTable.column(getParameter(labelColumn)).toDoubles()
    val features = dataTable.column(getParameter(featuresColumn)).toVector()
    require(labels.length == features.length)
    require(labels.length == dataTable.length)
    (labels, features)
  }

  def extractFeaturesAndLabelsArrays(dataTable: DataTable): (Array[Double], Array[Array[Double]]) = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)
    SmileUtil.featuresAndLabelsArrays(labels, features)
  }


  def extractFeaturesAndLabelsToSmileArrays(dataTable: DataTable): (Int, Array[Double], Option[Array[Array[Double]]], Option[Vector[SparseArray]]) = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)
    SmileUtil.extractFeaturesAndLabelsToSmileArrays(labels, features)
  }

  def extractWekaInstances(dataTable: DataTable, classification: Boolean = false,
                           nominalFeaturesCutoff: Int = -1, binary: Boolean = false): Instances = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)
    if (binary) {
      WekaUtil.featuresAndLabelsToBinaryInstances(labels, features, classification)
    } else {
      WekaUtil.featuresAndLabelsToInstances(labels, features, classification = classification,
        nominalFeaturesCutoff = nominalFeaturesCutoff)
    }
  }
}

trait HasPredictionColumn extends HasParameters {

  val predictionColumn = new Parameter[String](this, "predictionColumn", "The data table column containing predictions")

  def setPredictionColumn(value: String): this.type = setParameter(predictionColumn, value)

  setDefaultParameter(predictionColumn, "prediction")
}

trait HasProbabilityColumn extends HasParameters {

  val probabilityColumn = new Parameter[String](this, "probabilityColumn", "The probability or weight for the classification")

  def setProbabilityColumn(value: String): this.type = setParameter(probabilityColumn, value)

  setDefaultParameter(probabilityColumn, "probability")
}

trait HasPredictionParameters extends HasFeaturesParameters with HasPredictionColumn


trait PredictionModelWithProbabilities[M <: PredictionModelWithProbabilities[M]] extends Model[M, (Double, Double)] with HasPredictionParameters with HasProbabilityColumn {

  override def transform(dataTable: DataTable): DataTable = {

    val allFeatures = dataTable.column(getParameter(featuresColumn)).toVector()
    val (predictions, probabilities) = allFeatures.map {
      transformRow(_)
    }.unzip

    dataTable
      .addColumn(new DataTableColumn[Double](getParameter(predictionColumn), predictions.map(Some(_))))
      .addColumn(new DataTableColumn[Double](getParameter(probabilityColumn), probabilities.map(Some(_))))
  }


}

abstract class PredictorWithProbabilities[M <: PredictionModelWithProbabilities[M]]
  extends Estimator[M, (Double, Double)] with HasPredictionParameters with HasProbabilityColumn
