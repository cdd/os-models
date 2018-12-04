package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{HasFeaturesAndLabelParameters, Identifiable, PredictionModelWithProbabilities, PredictorWithProbabilities}
import weka.classifiers.trees.RandomForest
import weka.core.Instances

class WekaRandomForestClassifier(val uid: String)
  extends PredictorWithProbabilities[WekaRandomForestClassificationModel]
    with HasFeaturesAndLabelParameters with HasWekaRandomForestParameters {

  def this() = this(Identifiable.randomUID("weka_rfc"))

  override def fit(dataTable: DataTable): WekaRandomForestClassificationModel = {

    val rf = new RandomForest()

    rf.setMaxDepth(getParameter(maxDepth))
    rf.setNumExecutionSlots(getParameter(numThreads))
    // Weka RF - number of iterations is used to set number of trees
    rf.setNumIterations(getParameter(numTrees))
    val data = extractWekaInstances(dataTable, classification = true, nominalFeaturesCutoff = getParameter(nominalFeaturesCutoff))
    rf.buildClassifier(data)
    val header = new Instances(data, 0)
    copyParameterValues(new WekaRandomForestClassificationModel(uid, rf, header))
  }
}

class WekaRandomForestClassificationModel(override val uid: String, val rf: RandomForest, val header: Instances)
  extends PredictionModelWithProbabilities[WekaRandomForestClassificationModel] with HasFeaturesAndLabelParameters {

  override def transformRow(features: VectorBase): (Double, Double) = {
    WekaUtil.classificationWithProbability(rf, header, features)
  }
}