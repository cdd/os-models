package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._

trait MajorityVotingParameters extends HasParameters {
  val tieIsPositive = new Parameter[Boolean](this, "tieIsPositive", "Tie in classifiers is positive")

  def tieIsPositive(value: Boolean): this.type = setParameter(tieIsPositive, value)

  setDefaultParameter(tieIsPositive, true)
}

class MajorityVotingClassifier(val uid: String) extends Predictor[MajorityVotingClassificationModel]
  with HasFeaturesAndLabelParameters with MajorityVotingParameters {

  def this() = this(Identifiable.randomUID("voting_cm"))

  val classifiers = new Parameter[Vector[Pipeline]](this, "classifiers", "Classifiers")

  def setClassifiers(value: Vector[Pipeline]): this.type = setParameter(classifiers, value)

  override def fit(dataTable: DataTable): MajorityVotingClassificationModel = {
    val models = getParameter(classifiers).map { c => c.fit(dataTable) }
    copyParameterValues(new MajorityVotingClassificationModel(uid, models))
  }
}

class MajorityVotingClassificationModel(override val uid: String, val models: Vector[PipelineModel])
  extends PredictionModel[MajorityVotingClassificationModel]
    with MajorityVotingClassificationModelBase
    with HasFeaturesAndLabelParameters {


  override def transformRow(features: VectorBase): Double = {
    predict(features)
  }


}

trait MajorityVotingClassificationModelBase extends MajorityVotingParameters {
  val models: Vector[PipelineModel]

  private lazy val testFeatures = models.map {
    new TestFeatures(_)
  }

  def predict(features: VectorBase): Double = {
    var posCount = 0
    var negCount = 0

    testFeatures.foreach { tf =>
      val prediction = tf.predict(features)
      if (isPositive(prediction)) posCount += 1 else negCount += 1
    }

    if (getParameter(tieIsPositive)) {
      if (posCount >= negCount) 1.0 else 0.0
    } else {
      if (posCount > negCount) 1.0 else 0.0
    }

  }

  private def isPositive(prediction: Double) = {
    assert(prediction == 0.0 || prediction == 1.0)
    prediction == 1.0
  }
}
