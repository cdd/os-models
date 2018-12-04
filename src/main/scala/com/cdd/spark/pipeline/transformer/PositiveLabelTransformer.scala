package com.cdd.spark.pipeline.transformer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.functions._

class PositiveLabelTransformer(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("PositiveLabelTransformer"))

  final val inputCol = new Param[String](this, "inputCol", "The input column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  setDefault(inputCol, "label")

  final val outputCol = new Param[String](this, "outputCol", "The output column")

  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(outputCol, "positiveLabel")


  def copy(extra: ParamMap): PositiveLabelTransformer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), DoubleType, false)
    StructType(outputFields)
  }

  override def transform(df: Dataset[_]): DataFrame = {
    val minimum: Double = df.agg(min(df($(inputCol)))).head().getDouble(0)
    val scale = if (minimum <= 0) 1.0 - minimum else 0
    val scaler = udf { label: Double => label + scale }
    df.withColumn($(outputCol), scaler(df($(inputCol))))
  }
}
