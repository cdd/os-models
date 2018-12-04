package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import smile.regression._

trait HasSmileThreads extends HasParameters {
  val numThreads = new Parameter[Int](this, "numThreads", "Size of thread pool used in smile executors")
  def setNumThreads(value:Int):this.type = setParameter(numThreads, value)
  setDefaultParameter(numThreads, -1)

  def setSmileThreadPoolSize(): Unit = {
    // this will have no effect if the thread pool is already created
    System.setProperty("smile.threads", String.valueOf(getParameter(numThreads)))
  }
}

trait HasSmileRandomForestParameters extends HasParameters {

  val numTrees = new Parameter[Int](this, "numTrees", "Number of trees in the forest")

  def setNumTrees(value: Int): this.type = setParameter(numTrees, value)

  setDefaultParameter(numTrees, 60)

  val maxNodes = new Parameter[Int](this, "maxNodes", "Maximum number of nodes in a tree")

  def setMaxNodes(value: Int): this.type = setParameter(maxNodes, value)

  setDefaultParameter(maxNodes, 60)

}

class SmileRandomForestRegressor(val uid: String) extends Predictor[SmileRandomForestRegressionModel]
  with HasFeaturesAndLabelParameters with HasSmileThreads with HasSmileRandomForestParameters {

  def this() = this(Identifiable.randomUID("smile_rfr"))

  override def fit(dataTable: DataTable): SmileRandomForestRegressionModel = {
    val (y, x) = extractFeaturesAndLabelsArrays(dataTable)

    val rf = randomForest(x, y, ntrees = getParameter(numTrees), maxNodes=getParameter(maxNodes))
    //val rf = new RandomForest(x, y, getParameter(numTrees), getParameter(maxNodes), 5, x(0).length / 3)
    copyParameterValues(new SmileRandomForestRegressionModel(uid, rf))
  }
}

class SmileRandomForestRegressionModel(override val uid: String, val rf: RandomForest)
  extends PredictionModel[SmileRandomForestRegressionModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): Double = {
    rf.predict(features.toArray())
  }
}