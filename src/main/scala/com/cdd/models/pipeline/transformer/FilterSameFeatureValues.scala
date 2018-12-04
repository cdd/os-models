package com.cdd.models.pipeline.transformer


import com.cdd.models.datatable._
import com.cdd.models.pipeline._

import scala.collection.mutable

@SerialVersionUID(8865749889584043366L)
class FilterSameFeatureValues(override val uid: String) extends FeatureMapperEstimator[FilterSameFeatureValuesModel] {

  def this() = this(Identifiable.randomUID("filter_sfv"))

  setDefaultParameter(outputColumn, "uniqueFeatures")

  class MultipleValuesFinder() {
    val indicesToKeep = mutable.SortedSet[Int]()
    var first: Option[VectorBase] = None

    def update(v: VectorBase): this.type = {
      if (first == None) {
        first = Some(v)
      }
      else {
        updateIndicesToKeep(v)
      }
      this
    }

    private def updateIndicesToKeep(v: VectorBase) = {
      v match {
        case dv: DenseVector =>
          0 until dv.length() foreach { index =>
            require(!dv(index).isNaN)
            if (!indicesToKeep.contains(index) && dv(index) != first.get(index))
              indicesToKeep += index
          }
        case sv: SparseVector =>
          sv.indices().zip(sv.values()) foreach {
            case (index, value) =>
              require(!value.isNaN)
              if (!indicesToKeep.contains(index) && value != first.get(index))
                indicesToKeep += index
          }
          val firstSv = first.get.asInstanceOf[SparseVector]
          firstSv.indices().zip(firstSv.values()) foreach {
            case (index, value) =>
              if (!indicesToKeep.contains(index) && value != sv(index))
                indicesToKeep += index
          }
        case _ =>
          throw new IllegalArgumentException("Unsupported vector format. Expected breeze " +
            s"SparseVector or DenseVector. Instead got: ${v.getClass}")
      }
    }

  }

  override def fit(dataTable: DataTable): FilterSameFeatureValuesModel = {
    val finder = new MultipleValuesFinder()
    dataTable.column(getParameter(featuresColumn)).toVector().foreach(finder.update(_))
    val indices = finder.indicesToKeep.toVector
    copyParameterValues(new FilterSameFeatureValuesModel(uid, indices))
  }
}

class FilterSameFeatureValuesModel(override val uid: String, private val indices: Vector[Int]) extends FeatureMappingModel[FilterSameFeatureValuesModel] {

  override def transformRow(features: VectorBase): VectorBase = {
    features match {
      case features: DenseVector => Vectors.Dense(indices.map(features.apply).toArray)
      case features: SparseVector => features.slice(indices)
      case _ => throw new IllegalArgumentException("Unknown double vector type")
    }
  }

}
