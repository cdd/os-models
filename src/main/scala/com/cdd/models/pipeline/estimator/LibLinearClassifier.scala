package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{Identifiable, PredictionModelWithProbabilities, PredictorWithProbabilities}
import de.bwaldvogel.liblinear.{Model => LinearModel, Linear}

class LibLinearClassifier(val uid: String) extends PredictorWithProbabilities[LibLinearClassificationModel] with LibLinearEstimator {

  def this() = this(Identifiable.randomUID("linear_svc"))

    override def fit(dataTable: DataTable): LibLinearClassificationModel = {
      val linearModel = train(false, dataTable)

      copyParameterValues(new LibLinearClassificationModel(uid, linearModel))
    }
}

class LibLinearClassificationModel(val uid:String, val linearModel: LinearModel)
  extends PredictionModelWithProbabilities[LibLinearClassificationModel] with LibLinearModel{

   override def transformRow(features: VectorBase): (Double, Double) = {
     val linearFeatures = encodeFeatures(features)
     val values = Array(0.0)
     Linear.predictValues(linearModel, linearFeatures, values)
     // result is raw value
     val result = if (linearModel.getLabels()(0) == 1) values(0) else -values(0)
     val prediction = if(result>0) 1.0 else 0.0
     // use sigmoid function to map result to [0, 1] interval
     val probability  = 1.0/(1.0+Math.exp(-result))
     (prediction, probability)
  }
}