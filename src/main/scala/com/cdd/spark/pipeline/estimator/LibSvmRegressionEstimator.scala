package com.cdd.spark.pipeline.estimator

import com.cdd.spark.utils.DataFrameOps
import libsvm.{svm, svm_model, svm_parameter}
import org.apache.spark.ml.Predictor
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.regression.RegressionModel
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by gjones on 6/13/17.
  */
class LibSvmRegressionEstimator(override val uid: String) extends Predictor[Vector, LibSvmRegressionEstimator, LibSvmRegressionModel] with LibSvmEstimator {
  def this() = this(Identifiable.randomUID("LibSVMClassifier"))

  val epsilon = new Param[Double](this, "eps", "set the epsilon in the SVR loss function")

  def setEpsilon(value: Double): this.type = set(epsilon, value)

  setDefault(epsilon, 0.1)

  def copy(extra: ParamMap): LibSvmRegressionEstimator = {
    defaultCopy(extra)
  }

  override def train(ds: Dataset[_]): LibSvmRegressionModel = {

    val data = DataFrameOps.singlePartitionToFeatureArray(ds, $(labelCol), $(featuresCol), $(forceCoalesce))
    val noFeatures = data(0)._2.size

    val svmProblem = buildProblem(data)

    val param = buildParams()
    param.svm_type = svm_parameter.EPSILON_SVR
    param.p = $(epsilon)
    param.probability = 0

    val check = svm.svm_check_parameter(svmProblem, param)
    if (check != null) {
      throw new IllegalArgumentException(s"SVM parameter error: ${check}")
    }
    val model = svm.svm_train(svmProblem, param)
    new LibSvmRegressionModel(uid, model, numFeatures.get)
  }

}

class LibSvmRegressionModel(override val uid: String, private val svmModel: svm_model, override val numFeatures: Int) extends RegressionModel[Vector, LibSvmRegressionModel] with LibSvmModel {

  override def copy(extra: ParamMap): LibSvmRegressionModel = {
    val copied = copyValues(new LibSvmRegressionModel(uid, svmModel, numFeatures),
      extra)
    copied.setParent(parent)
  }

  protected def predict(features: Vector): Double = {
    features.toArray.zipWithIndex.foreach { case (v, index) =>
      predictNodes(index).value = v
    }
    val prediction = svm.svm_predict(svmModel, predictNodes)
    prediction
  }

}