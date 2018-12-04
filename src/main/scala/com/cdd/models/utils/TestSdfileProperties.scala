package com.cdd.models.utils

import com.cdd.models.datatable.ActivityTransform

/**
  * A class to store property information about an SDF file, activity tag and transform.  Instantiate from JSON
  *
  * @param file      SDF file
  * @param response  Name of activity field
  * @param category  Discrete or continuous
  * @param transform A transform to be applied to raw response (can be null rather than use an Option as it is read using gson)
  */
case class TestSdfileProperties(file: String, response: String, category: String, transform: String) {

  /**
    * Convert transform name to transform function
    *
    * @return
    */
  def activityTransform(): Option[ActivityTransform] = {
    ActivityTransform.stringToTransform(transform)
  }

  /**
    * True if the data set is continuous
    *
    * @return
    */
  def continuous(): Boolean = {
    category match {
      case "continuous" => true
      case "discrete" => false
      case _ => throw new IllegalArgumentException(s"Illegal category value for ${category}")
    }
  }

  /**
    * Transform an SD field value to a real value
    *
    * @param fieldValue
    * @return
    */
  def fieldToActivity(fieldValue: String): Option[Double] = {
    toActivityField().fieldToActivity(fieldValue)
  }

  def toActivityField(): SdfileActivityField = {
    val cat = if (continuous()) ActivityCategory.CONTINUOUS else ActivityCategory.DISCRETE
    SdfileActivityField(response, cat, activityTransform())
  }
}

object ActivityCategory extends Enumeration {
  type ActivityCategory = Value
  val CONTINUOUS, DISCRETE = Value
}

case class SdfileActivityField(fieldName: String, category: ActivityCategory.Value, transform: Option[ActivityTransform]) {

  def fieldToActivity(fieldValue: String): Option[Double] = {
    fieldValue match {
      case null => None
      case "" => None
      case field =>
        var rawValue =
          if (category == ActivityCategory.CONTINUOUS)
            Util.activityContinuousValue(field)
          else Util.activityCategoryValue(field)
        if (category == ActivityCategory.CONTINUOUS && transform != None)
          transform.get.transformActivity(rawValue)
        else rawValue
    }
  }

  def fieldOptionToActivity(fieldValue: Option[String]): Option[Double] = {
    fieldValue match {
      case Some(v) => fieldToActivity(v)
      case None => None
    }
  }

}