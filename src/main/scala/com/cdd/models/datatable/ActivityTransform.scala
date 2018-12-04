package com.cdd.models.datatable

import java.lang.Math._

import com.cdd.models.utils.HasLogging
import org.apache.log4j.Logger

case class ActivityTransform(activityTransformFunction: (Double) => Double) extends HasLogging {

  def transformActivity(activities: List[Option[Double]]): List[Option[Double]] = {
    activities.map { transformActivity(_)}
  }

  def transformActivity(activity: Option[Double]): Option[Double] = {
    activity match {
      case Some(v) =>
        val tv = activityTransformFunction(v)
        if (tv.isNaN || tv.isInfinite) {
          logger.warn(s"Error transforming activity value $v")
          None
        } else {
          Some(tv)
        }
      case None => None
    }
  }
}

object ActivityTransform {

  val transformUmolIc50 = ActivityTransform((ic50: Double) => -log10(ic50 * 1e-6))
  val transformNmolIc50 = ActivityTransform((ic50: Double) => -log10(ic50 * 1e-9))
  val transformToLog10 = ActivityTransform((value: Double) => log10(value))

  /**
    * Convert transform name to transform function
    *
    * @return
    */
  def stringToTransform(transform: String): Option[ActivityTransform] = {
    transform match {
      case "uM_to_pIc50" => Some(ActivityTransform.transformUmolIc50)
      case "nM_to_pIc50" => Some(ActivityTransform.transformNmolIc50)
      case "log10" => Some(ActivityTransform.transformToLog10)
      case null => None
      case "" => None
      case "none" => None
      case _ => throw new IllegalArgumentException(s"Illegal transform value for ${transform}")
    }
  }
}