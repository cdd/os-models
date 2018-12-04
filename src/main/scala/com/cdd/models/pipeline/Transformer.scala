package com.cdd.models.pipeline

import com.cdd.models.datatable.{DataTable, DataTableColumn, ParallelDataTable, VectorBase}

@SerialVersionUID(3683446307120803044L)
abstract class Transformer extends PipelineStage {

  def transform(dataTable: DataTable, parameterMap: ParameterMap): DataTable = {
    this.copy(parameterMap).transform(dataTable)
  }

  /**
    * Transforms the input data table.
    */
  def transform(dataTable: DataTable): DataTable

  def parallelTransform(dataTable: DataTable, chunkSize:Int): DataTable = {
    val fn: DataTable => DataTable = dt => {
      transform(dt)
    }
    ParallelDataTable.process(dataTable, fn, chunkSize)
  }


}
abstract class RowTransformer[F, T] extends Transformer {

  def transformRow(features: F): T
}

trait HasFeaturesParameters extends HasParameters {

  val featuresColumn = new Parameter[String](this, "featuresColumn", "The data table column containing feature vectors")

  def setFeaturesColumn(value: String): this.type = setParameter(featuresColumn, value)

  setDefaultParameter(featuresColumn, "features")

  def getFeaturesColumn(): String = getParameter(featuresColumn)

}

trait HasOutputColumn extends HasFeaturesParameters {

  val outputColumn = new Parameter[String](this, "outputColumn", "The data table column containing transformed feature vectors")

  def setOutputColumn(value: String): this.type = setParameter(outputColumn, value)

  setDefaultParameter(outputColumn, "transformedFeatures")

  def getOutputColumn(): String = getParameter(outputColumn)
}

trait HasOutputLabelColumn extends HasLabelColumn {

  val outputLabelColumn = new Parameter[String](this, "outputLabelColumn", "The data table column containing transformed labels")

  def setOutputLabelColumn(value: String): this.type = setParameter(outputLabelColumn, value)

  setDefaultParameter(outputLabelColumn, "transformedLabels")

}



trait FeatureMappingRowTransformer extends RowTransformer[VectorBase, VectorBase] with HasOutputColumn {

  override def transform(dataTable: DataTable): DataTable = {

    val allFeatures = dataTable.column(getParameter(featuresColumn)).toVector()
    val predictions = allFeatures.map {
      transformRow
    }

    dataTable.addColumn(new DataTableColumn[VectorBase](getParameter(outputColumn), predictions.map(Some(_))))
  }

  def transformRow(features: VectorBase): VectorBase
}

