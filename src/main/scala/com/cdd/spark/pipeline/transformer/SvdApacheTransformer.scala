package com.cdd.spark.pipeline.transformer

import com.cdd.models.utils.HasLogging
import com.cdd.spark.utils.DataFrameOps._
import com.google.common.primitives.Doubles
import org.apache.commons.math3.linear.SingularValueDecomposition
import org.apache.log4j.Logger
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrix => OldMatrix, Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by gjones on 6/19/17.
  */

object SvdApacheTransformer extends HasLogging{
}


class SvdApacheTransformer(override val uid: String) extends Estimator[SvdModel] with SvdTransformerParams with HasLogging {

  def this() = this(Identifiable.randomUID("ApacheSvdTransformer"))

  def copy(extra: ParamMap): SvdApacheTransformer =  {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), VectorType, false)
    StructType(outputFields)
  }

  override def fit(df: Dataset[_]): SvdModel = {

    if (df.rdd.partitions.length > 1) {
      val msg = "Performing Apache Math SVD on dataset with multiple partitions"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val matrix = rowMatrixToRealMatrix(dataFrameColumnToRowMatrix(df, $(inputCol)))
    val svd = new SingularValueDecomposition(matrix)
    var V = svd.getV
    V = V.getSubMatrix(0, V.getRowDimension-1, 0, $(k)-1)
    val vd = new DenseMatrix(V.getRowDimension, V.getColumnDimension, Doubles.concat(V.getData():_*))
    copyValues(new SvdModel(uid, vd)).setParent(this)
  }

}


