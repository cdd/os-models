package com.cdd.spark.pipeline.transformer

import com.cdd.spark.utils.DataFrameOps._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{SingularValueDecomposition, DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrix => OldMatrix, Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created by gjones on 6/19/17.
  */

trait SvdTransformerParams extends Params {
  final val k: IntParam = new IntParam(this, "k", "the number of principal components (> 0)",
    ParamValidators.gt(0))

  def setK(value: Int): this.type = set(k, value)

  setDefault(k, 10)

  final val inputCol = new Param[String](this, "inputCol", "The input column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  setDefault(inputCol, "features")

  final val outputCol = new Param[String](this, "outputCol", "The output column")

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(outputCol, "svdFeatures")

}

class SvdMlibTransformer(override val uid: String) extends Estimator[SvdModel] with SvdTransformerParams {


  def this() = this(Identifiable.randomUID("MlibSvdTransformer"))

  def copy(extra: ParamMap): SvdMlibTransformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

  override def fit(df: Dataset[_]): SvdModel = {
    val mat: RowMatrix = dataFrameColumnToRowMatrix(df, $(inputCol))
    val svd: SingularValueDecomposition[RowMatrix, OldMatrix] = mat.computeSVD($(k), computeU = false)
    val V = svd.V.asML.asInstanceOf[DenseMatrix]
    copyValues(new SvdModel(uid, V)).setParent(this)
  }

}

class SvdModel(override val uid: String, val V:DenseMatrix) extends Model[SvdModel] with SvdTransformerParams {

   override def copy(extra: ParamMap): SvdModel = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

  override def transform(df: Dataset[_]): DataFrame = {
    val transformUdf = udf { (v: Vector) => transformVector(v, V.asInstanceOf[DenseMatrix]) }
    df.withColumn($(outputCol), transformUdf(df($(inputCol))))
  }

  def transformVector(vector: Vector, V: DenseMatrix): DenseVector = {
    vector match {
      case dv: DenseVector =>
        V.transpose.multiply(dv)
      case SparseVector(size, indices, values) =>
        /* SparseVector -> single row SparseMatrix */
        val sm = Matrices.sparse(size, 1, Array(0, indices.length), indices, values).transpose
        val projection = sm.multiply(V)
        Vectors.dense(projection.values).asInstanceOf[DenseVector]
      case _ =>
        throw new IllegalArgumentException("Unsupported vector format. Expected " +
          s"SparseVector or DenseVector. Instead got: ${vector.getClass}")
    }
  }

}
