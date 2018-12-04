package com.cdd.spark.pipeline.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.sql.{DataFrame, Dataset}

object VectorFolder {
  private def foldSparse(v: SparseVector, foldSize: Int): Vector = {
     val foldedValues = Array.fill(foldSize) {
      .0
    }

    v.indices.zip(v.values).foreach { case(i, v) =>
      val fold = i % foldSize
        foldedValues(fold) += v
    }

    Vectors.dense(foldedValues)
  }

  private def foldDense(v: DenseVector, foldSize: Int): Vector = {
     val foldedValues = Array.fill(foldSize) {
      .0
    }

    v.values.zipWithIndex.foreach { case(v, i) =>
      val fold = i % foldSize
        foldedValues(fold) += v
    }

    foldedValues.foreach { v => assert(v >= 0, "Folded value must be positive")}
    Vectors.dense(foldedValues)
  }

}


import VectorFolder.{foldSparse, foldDense}

/**
  * Created by gjones on 7/8/17.
  */
class VectorFolder(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("SparseVectorFolder"))

  final val inputCol = new Param[String](this, "inputCol", "The input column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  setDefault(inputCol, "features")

  final val outputCol = new Param[String](this, "outputCol", "The output column")

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(outputCol, "foldedFeatures")

  final val foldSize = new Param[Int](this, "foldSize", "Size to fold sparse vector in")

   def setFoldSize(value: Int): this.type = set(foldSize, value)

  setDefault(foldSize, 512)


  def copy(extra: ParamMap): VectorFolder = {
    defaultCopy(extra)
  }
  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

    override def transform(df: Dataset[_]): DataFrame = {
      val size = $(foldSize)
     val folder = udf { vec: Vector =>
       vec match {
         case features: DenseVector => foldDense(features, size)
         case features: SparseVector => foldSparse(features, size)
       }
     }
     df.withColumn($(outputCol), folder(df($(inputCol))))
  }

}
