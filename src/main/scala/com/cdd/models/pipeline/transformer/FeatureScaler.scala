package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable.{DataTable, SparseVector, VectorBase}
import com.cdd.models.pipeline._

@SerialVersionUID(8868781867021476011L)
class FeatureScaler(override val uid: String) extends FeatureMapperEstimator[FeatureScalerModel] {

  val scalerType = new Parameter[String](this, "scalerType", "Type of feature scaling (MaxMin or Standard")

  def setScalerType(value: String):this.type = setParameter(scalerType, value)

  setDefaultParameter(scalerType, "Standard")

  setDefaultParameter(outputColumn, "normalizedFeatures")

  def this() = this(Identifiable.randomUID("feature_scaler"))

  override def fit(dataTable: DataTable): FeatureScalerModel = {
    val sparse = dataTable.column(getParameter(featuresColumn)).toVector()(0).isInstanceOf[SparseVector]
    val model = getParameter(scalerType) match {
      case "MaxMin" => copyParameterValues(new MaxMinScaler()).fit(dataTable)
      case "Standard" =>
        val scaler = new StandardScaler()
        if (sparse)
          scaler.setCenter(false)
        copyParameterValues(scaler).fit(dataTable)
      case "Raw" => new IdentityTransformer()
      case _ => throw new IllegalArgumentException
    }
    copyParameterValues(new FeatureScalerModel(uid, model))
  }
}

class IdentityTransformer(override val uid:String) extends FeatureMappingRowTransformer  {
  def this() = this(Identifiable.randomUID("identity"))
   override def transformRow(features: VectorBase): VectorBase = {
    features
  }
}

class FeatureScalerModel(override val uid: String, private val model: FeatureMappingRowTransformer)
  extends FeatureMappingModel[FeatureScalerModel] {

  override def transformRow(features: VectorBase): VectorBase = {
    model.transformRow(features)
  }
}