package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import weka.classifiers.trees.RandomForest
import weka.core.Instances


trait HasWekaRandomForestParameters extends  HasCommonWekaParameters{

  val numTrees = new Parameter[Int](this, "numTrees", "Number of trees in forest")

  def setNumTrees(value: Int): this.type = setParameter(numTrees, value)

  setDefaultParameter(numTrees, 10)

  val maxDepth = new Parameter[Int](this, "maxDepth", "Maximum depth of trees")

  def setMaxDepth(value: Int): this.type = setParameter(maxDepth, value)

  setDefaultParameter(maxDepth, 30)

  val numThreads = new Parameter[Int](this, "numThreads", "Number of execution slots")

  def setNumThreads(value: Int): this.type = setParameter(numThreads, value)

  setDefaultParameter(numThreads, 0)


}

@SerialVersionUID(5383617949121667368L)
class WekaRandomForestRegressor(val uid: String)
  extends Predictor[WekaRandomForestRegressionModel]
    with HasFeaturesAndLabelParameters with HasWekaRandomForestParameters {

  def this() = this(Identifiable.randomUID("weka_rfr"))

  override def fit(dataTable: DataTable): WekaRandomForestRegressionModel = {

    val labelDimension = this.labelDimensionality(dataTable)
    val originalWekaSmall = weka.core.Utils.SMALL
    assert(originalWekaSmall == 1.0e-6)
    if (labelDimension < 1) {
      val wekaTol = originalWekaSmall * labelDimension
      logger.info(s"Setting weka SMALL comparison tolerance to $wekaTol")
      weka.core.Utils.SMALL = wekaTol
    }

    val rf = new RandomForest()

    rf.setMaxDepth(getParameter(maxDepth))
    rf.setNumExecutionSlots(getParameter(numThreads))
    // Weka RF - number of iterations is used to set number of trees
    rf.setNumIterations(getParameter(numTrees))
    val data = extractWekaInstances(dataTable, classification = false, nominalFeaturesCutoff = getParameter(nominalFeaturesCutoff))
    rf.buildClassifier(data)
    weka.core.Utils.SMALL = originalWekaSmall
    val header = new Instances(data, 0)
    copyParameterValues(new WekaRandomForestRegressionModel(uid, rf, header))
  }
}

class WekaRandomForestRegressionModel(override val uid: String, val rf: RandomForest, val header: Instances)
  extends PredictionModel[WekaRandomForestRegressionModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): Double = {
    WekaUtil.predict(rf, header, features)
  }
}