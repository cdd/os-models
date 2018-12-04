package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.{HasParameters, Identifiable, Parameter, Transformer}

/**
  * A trait for a column containing active compounds.  Active compounds should be labeled 1 and inactive 0
  */
trait HasActivityColumn extends HasParameters {
  val activityColumn = new Parameter[String](this, "activityColumn", "Binary column containing activity prediction")
  def setActivityColumn(value:String): this.type = setParameter(activityColumn, value)
  setDefaultParameter(activityColumn, "prediction")
}

class ActivityFilter(override val uid: String) extends Transformer with HasActivityColumn {
  def this() = this(Identifiable.randomUID("actFilter"))


  override def transform(dataTable: DataTable): DataTable = {
    val(inactive, active) = dataTable.classifierPredictionSplit(getParameter(activityColumn))
    active
  }

}
