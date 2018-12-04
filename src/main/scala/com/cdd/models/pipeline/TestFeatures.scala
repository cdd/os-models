package com.cdd.models.pipeline

import com.cdd.models.datatable.{DataTable, DataTableColumn, VectorBase}

@SerialVersionUID(1234L)
class TestFeatures(model:PipelineModel) extends Serializable {
  import TestFeatures._

   def this(modelFileName: String) =
    this(PipelineModel.loadPipeline(modelFileName))

  def predict(featuresArray:Vector[VectorBase]): Vector[Double] = {
    val featuresColumn = model.getFeaturesColumn()
    val dataTable = featuresArrayToDataTable(featuresArray, featuresColumn)
    val predictDt = model.transform(dataTable)
    require(predictDt.hasColumnWithName("prediction"))
    predictDt.column("prediction").toDoubles()
  }

  def predict(features: VectorBase): Double = {
    val predictArray = predict(Vector(features))
    assert(predictArray.size == 1)
    predictArray(0)
  }

}

object TestFeatures {

  def featuresArrayToDataTable(features:Vector[VectorBase], featuresColumnName:String="features") :DataTable = {
    val column = DataTableColumn.fromVector(featuresColumnName, features)
    new DataTable(Vector(column))
  }
}
