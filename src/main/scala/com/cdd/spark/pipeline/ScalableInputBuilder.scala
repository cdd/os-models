package com.cdd.spark.pipeline

import com.cdd.models.utils.{Configuration, HasLogging, TestSdfileProperties}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}



case class MoleculeInfo(no: Int, smiles: String)

/**
  * Base class for building dataframes of ML information from SDF files
  */
abstract class ScalableInputBuilder[B <: ScalableInputBuilder[B, M], M](val path: String, activityFields: List[TestSdfileProperties],
                                                                        val resource: Boolean, val addActivities: Boolean, val prefix: String)
extends HasLogging {


  protected var df: Option[DataFrame] = None

  protected def addFingerprintColumns(): this.type

  protected def addDescriptorColumns(): this.type

  protected def activityFieldValue(moleculeNo: Int, field: String): Option[String]

  protected val molecules: Seq[Option[M]] = loadMolecules()

  def noMolecules(): Int = molecules.length

  protected def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo]

  protected def loadMolecules(): Seq[Option[M]]

  def build: DataFrame = {
    buildDataFrame
    addFingerprintColumns()
    addDescriptorColumns()
    df.get
  }

  def buildDataFrame: this.type = {

    val nColumns = if (addActivities) 2 + activityFields.size * 2 else 2

    logger.warn("building base datatable")
    val data = (0 until noMolecules()).map { index =>
      val values = new Array[Any](nColumns)
      values(0) = index
      val info = moleculeInfo(index)
      values(1) = if (info != None) info.get.smiles else null
      if (addActivities) {
        activityFields.zipWithIndex.foreach { case (activityField, no) =>
          val field = activityField.response
          val fieldValue = if (info != None) activityFieldValue(index, field) else None
          if (fieldValue != None && !fieldValue.get.isEmpty) {
            val doubleValue = activityFields(no).fieldToActivity(fieldValue.get)
            if (doubleValue == None) {
              logger.warn(s"unable to get float value from field ${fieldValue.get}")
            }
            values(2 + no * 2) = fieldValue.get
            values(2 + no * 2 + 1) = doubleValue.getOrElse(null)
          }
          else {
            values(2 + no * 2) = null
            values(2 + no * 2 + 1) = null
          }
        }
      }
      logger.debug(s"Adding row of values ${values.mkString("|")}")
      Row(values: _*)
    }

    logger.warn("Built rows")
    val columnNames = new Array[String](nColumns)
    columnNames(0) = "no"
    columnNames(1) = s"${prefix}_smiles"

    val structFields = new Array[StructField](nColumns)
    structFields(0) = new StructField("no", IntegerType, nullable = false)
    structFields(1) = new StructField(s"${prefix}_smiles", StringType, nullable = true)

    if (addActivities) {
      logger.warn("Adding activities")
      (0 until activityFields.size).foreach { no =>
        var title = no match {
          case 0 => "activity_string"
          case _ => s"activity_string_${no + 1}"
        }
        columnNames(2 + no * 2) = title
        structFields(2 + no * 2) = new StructField(title, StringType, nullable = true)

        title = no match {
          case 0 => "activity_value"
          case _ => s"activity_value_${no + 1}"
        }
        columnNames(2 + no * 2 + 1) = title
        structFields(2 + no * 2 + 1) = new StructField(title, DoubleType, nullable = true)
      }
    }

    logger.warn("Creating dataframe")
    val df = Configuration.sparkSession.createDataFrame(Configuration.sparkContext.parallelize(data, 4), new StructType(structFields)).toDF(columnNames: _*)
    logger.warn("Created dataframe")

    this.df = Some(df)
    this
  }

  def getDf: DataFrame = df.get
}