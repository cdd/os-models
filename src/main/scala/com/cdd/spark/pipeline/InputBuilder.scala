package com.cdd.spark.pipeline

import breeze.numerics.log10
import com.cdd.models.datatable.ActivityTransform
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.Util
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.reflect.runtime.universe._





/**
  * Created by gjones on 6/5/17.
  */
abstract class InputBuilder[B <: InputBuilder[B]](val path: String, val resource: Boolean = false) {

  protected var activityField = "activity"
  protected var activityTransform: Option[ActivityTransform] = None
  protected var df: Option[DataFrame] = None
  protected var foldSize = 1024
  protected var fingerprintTransform = FingerprintTransform.Sparse
  private var activityNo = 1

  def setActivityTransform(activityTransform: ActivityTransform): this.type = {
    this.activityTransform = Some(activityTransform)
    this
  }

  def setActivityTransform(activityTransform: Option[ActivityTransform]): this.type = {
    this.activityTransform = activityTransform
    this
  }

  def setActivityField(activityField: String): this.type = {
    this.activityField = activityField
    this
  }

  protected def addColumn[T: TypeTag](name: String, func: Int => T): this.type = {
    val sqlfunc = functions.udf(func)
    val df = this.df.get.withColumn(name, sqlfunc(functions.col("no")))
    this.df = Some(df)
    this
  }

  def getDf(): DataFrame = df.get

  def firstFingerprintColumn(): String = {
    df.get.columns.collectFirst { case column if column.startsWith("fingerprints") => column }.get
  }

  private def processActivity(field: String, discrete: Boolean) = {
    val activityValues = extractActivityStrings(field)
    var activityDoubleValues: List[Option[Double]] = activityValues.map { case Some(a) => if (discrete) Util.activityCategoryValue(a) else Util.activityContinuousValue(a) case _ => None }
    if (!discrete && activityTransform != None) {
      activityDoubleValues = activityTransform.get.transformActivity(activityDoubleValues)
    }
    (activityValues, activityDoubleValues)
  }

  def addActivityColumn(field: String, discrete: Boolean = false): this.type = {
    activityNo += 1
    val (activityValues, activityDoubleValues) = processActivity(field, discrete)

    val func1: (Int => String) = (no) => {
      activityValues(no).getOrElse(null)
    }
    val func2: (Int => java.lang.Double) = (no) => {
      Util.toJavaDouble(activityDoubleValues(no))
    }

    addColumn(s"activity_string_${activityNo}", func1)
    addColumn(s"activity_value_${activityNo}", func2)
  }

  def setFoldSize(foldSize: Int): this.type = {
    this.foldSize = foldSize
    this
  }

  def setFingerprintTransform(fingerprintTransform: FingerprintTransform.Value): this.type = {
    this.fingerprintTransform = fingerprintTransform
    this
  }

  def addSmilesColumn(): this.type

  def addDescriptorsColumn(): this.type

  def build(sparkSession: SparkSession, discrete: Boolean = false): this.type = {
    readMolecules()
    val (activityValues, activityDoubleValues) = processActivity(activityField, discrete)
    val data: Seq[(Int, String, java.lang.Double)] = (activityValues zip activityDoubleValues).zipWithIndex
      .map { case ((as, ad), i) => (i, as.getOrElse(null), Util.toJavaDouble(ad)) }
    val df = sparkSession.createDataFrame(data).toDF("no", "activity_string", "activity_value")
    this.df = Some(df)
    this
  }

  def append(inputBuilder: InputBuilder[_]): this.type = {
    if (inputBuilder.path != path || inputBuilder.resource != resource)
      throw new IllegalArgumentException("append: sdfiles do not match")
    readMolecules()
    df = Some(inputBuilder.getDf())
    this
  }

  def readMolecules(): Unit

  def addFingerprintColumn(): this.type

  protected def extractActivityStrings(field: String): List[Option[String]]
}
