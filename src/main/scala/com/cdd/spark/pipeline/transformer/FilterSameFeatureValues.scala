package com.cdd.spark.pipeline.transformer

import com.cdd.models.utils.Util
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues.MultipleValuesAccumulator
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable.SortedSet

object FilterSameFeatureValues {

  @SerialVersionUID(1000L)
  class MultipleValuesAccumulator() extends Serializable {
    val indicesToKeep = SortedSet[Int]()
    var first: Option[Vector] = None

    def update(v: Vector): this.type = {
      if (first == None) {
        first = Some(v)
      }
      else {
        updateIndicesToKeep(v)
      }
      this
    }

    private def updateIndicesToKeep(v: Vector) = {
      v match {
        case dv: DenseVector =>
          0 until dv.size foreach { index =>
            if (!indicesToKeep.contains(index) && dv(index) != first.get(index))
              indicesToKeep += index
          }
        case sv: SparseVector =>
          sv.indices.zip(sv.values) foreach {
            case (index, value) =>
              if (!indicesToKeep.contains(index) && value != first.get(index))
                indicesToKeep += index
          }
          val firstSv = first.get.asInstanceOf[SparseVector]
          firstSv.indices.zip(firstSv.values) foreach {
            case (index, value) =>
              if (!indicesToKeep.contains(index) && value != sv(index))
                indicesToKeep += index
          }
        case _ =>
          throw new IllegalArgumentException("Unsupported vector format. Expected " +
            s"SparseVector or DenseVector. Instead got: ${v.getClass}")
      }
    }


    def merge(other: MultipleValuesAccumulator): this.type = {
      if (first == None) {
       first = other.first
        indicesToKeep ++= other.indicesToKeep
      }
      else if (other.first != None) {
        indicesToKeep ++= other.indicesToKeep
        updateIndicesToKeep(other.first.get)
      }
      this
    }
  }


}

trait FilterSameFeatureValuesParams extends Params {

  final val inputCol = new Param[String](this, "inputCol", "The input column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  setDefault(inputCol, "features")

  final val outputCol = new Param[String](this, "outputCol", "The output column")

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(outputCol, "uniqueFeatures")

}

/**
  * Created by gjones on 6/20/17.
  */
class FilterSameFeatureValues(override val uid: String) extends Estimator[FilterSameFeatureValuesModel] with FilterSameFeatureValuesParams {

  def this() = this(Identifiable.randomUID("FilterSameVectorSlicer"))

  def copy(extra: ParamMap): FilterSameFeatureValues = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

  override def fit(df: Dataset[_]): FilterSameFeatureValuesModel = {
    val indicesToKeep = df.select($(inputCol)).rdd.aggregate(new MultipleValuesAccumulator)(
      seqOp = (counter, row) => {
        counter.update(row(0).asInstanceOf[Vector])
      },
      combOp = (counter, other) => {
        counter.merge(other)
      }).indicesToKeep.toArray

     copyValues(new FilterSameFeatureValuesModel(uid, indicesToKeep)).setParent(this)
  }
}

class FilterSameFeatureValuesModel(override val uid: String, indices: Array[Int]) extends Model[FilterSameFeatureValuesModel] with FilterSameFeatureValuesParams {

  def copy(extra: ParamMap): FilterSameFeatureValuesModel = {
    val copied = copyValues(new FilterSameFeatureValuesModel(uid, indices),
      extra)
    copied.setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

   override def transform(df: Dataset[_]): DataFrame = {

     val slicer = udf { vec: Vector =>
       vec match {case features: DenseVector => Vectors.dense(indices.map(features.apply))
         case features: SparseVector => Util.slice(features, indices)
       }
     }
     df.withColumn($(outputCol), slicer(df($(inputCol))))
  }

}