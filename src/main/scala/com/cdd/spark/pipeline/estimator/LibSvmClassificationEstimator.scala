package com.cdd.spark.pipeline.estimator

import com.cdd.spark.utils.DataFrameOps
import libsvm._
import org.apache.spark.ml.Predictor
import org.apache.spark.ml.classification.ClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset


class LibSvmClassificationEstimator(override val uid: String) extends Predictor[Vector, LibSvmClassificationEstimator, LibSvmClassificationModel] with LibSvmEstimator {
  def this() = this(Identifiable.randomUID("LibSVMClassifier"))

  def copy(extra: ParamMap): LibSvmClassificationEstimator = {
    defaultCopy(extra)
  }

  override def train(ds: Dataset[_]): LibSvmClassificationModel = {

    val data = DataFrameOps.singlePartitionToFeatureArray(ds, $(labelCol), $(featuresCol), $(forceCoalesce))
    val svmProblem = buildProblem(data)

    val param = buildParams()
    param.svm_type = svm_parameter.C_SVC
    param.probability = 1

    val model = svm.svm_train(svmProblem, param)
    new LibSvmClassificationModel(uid, model, numFeatures.get)
  }

}

class LibSvmClassificationModel(override val uid: String, private val svmModel: svm_model, override val numFeatures: Int) extends ClassificationModel[Vector, LibSvmClassificationModel] with LibSvmModel {

  def numClasses: Int = 2

  protected def predictRaw(features: Vector): Vector = {
    features.toArray.zipWithIndex.foreach { case (v, index) =>
      predictNodes(index).value = v
    }
    val probabilities = new Array[Double](2)
    val prediction = svm.svm_predict_probability(svmModel, predictNodes, probabilities)
    if (svmModel.label(0) == 1) {
      Vectors.dense(probabilities.reverse)
    } else {
      Vectors.dense(probabilities)
    }
  }


  override def copy(extra: ParamMap): LibSvmClassificationModel = defaultCopy(extra)

}
