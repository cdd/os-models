package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.pipeline._

class ActivityScaler(override val uid:String) extends Transformer with HasActivityColumn with HasLabelColumn with HasOutputLabelColumn {
  def this() = this(Identifiable.randomUID("actScaler"))

  val scaleStep = new Parameter[Double](this, "scaleStep", "Delta to add to the activity of inactive compounds (0 to exclude inactive compounds)")
  def setScaleStep(value: Double): this.type = setParameter(scaleStep, value)
  setDefaultParameter(scaleStep, 1.0)

  val excludeCompounds = new Parameter[Boolean](this, "excludeCompounds", "Filter inactive compounds (rather than shifting them)")

  override def transform(dataTable: DataTable): DataTable = {

    if (getParameter(excludeCompounds)) {

      val filter = copyParameterValues(new ActivityFilter())
      val filteredTable = filter.transform(dataTable)
      val values = filteredTable.column(getParameter(labelColumn)).asInstanceOf[DataTableColumn[Double]].values
      val newLabelColum = new DataTableColumn[Double](getParameter(outputLabelColumn), values)
      filteredTable.addColumn(newLabelColum)

    } else {

      val step = getParameter(scaleStep)
      val allLabels = dataTable.column(getParameter(labelColumn)).toDoubles()
      val allActivities = dataTable.column(getParameter(activityColumn)).values

      val newLabels = allLabels.zip(allActivities).map {
        case (l, a) =>
          val activity = a.asInstanceOf[Option[Int]]
          activity match {
            case Some(0) => l + step
            case _ => l
          }

      }

      dataTable.addColumn(new DataTableColumn[Double](getParameter(outputLabelColumn), newLabels.map(Some(_))))
    }
  }


}
