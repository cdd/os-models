package com.cdd.models.molecule

import com.cdd.models.utils.{HasLogging, Util}
import org.apache.log4j.Logger
import org.openscience.cdk.interfaces.IAtomContainer

import scala.collection.mutable
import scala.collection.JavaConversions._

class CdkDataTableBuilder(fileName: String) extends HasLogging {

  var molecules: Vector[Option[IAtomContainer]] = CdkMoleculeReader.readMolecules(fileName, resource = false).toVector

  def allProperties(): Set[String] = {
    val properties = molecules.foldLeft(mutable.Set[String]()) { (props, m) =>
      m match {
        case Some(mol) =>
          val molProps = mol.getProperties.keySet().toSet.map((p: Object) => p.toString)
          props ++= molProps
        case _ =>
          props
      }
    }
    properties.toSet
  }

  def propertyValues(property: String): Vector[Option[String]] = {

    molecules.map { m =>
      m match {
        case Some(m) =>
          if (m.getProperties.containsKey(property))
            Some(m.getProperty(property))
          else None
        case _ => None
      }
    }
  }

  def doublePropertyValues(property: String): Option[Vector[Option[Double]]] = {
    val stringValues = propertyValues(property)
    val nStringValues = stringValues.count(_ != None)

    val doubleValues = stringValues.map(_ match {
      case Some(sv) =>
        Util.activityContinuousValue(sv)
      case _ => None
    })

    val nDoubleValues = doubleValues.count(_ != None)
    val proportion = nDoubleValues.toDouble / nStringValues.toDouble
    logger.warn(s"Proportion of double values for field ${property} in file ${fileName} is ${proportion} [n string ${nStringValues} n double ${nDoubleValues}")

    if (proportion < 0.1)
      return None

    stringValues.zip(doubleValues)
      .filter { case (sv, _) => sv != None }
      .foreach { case (sv, dv) =>
        if (dv == None)
          logger.warn(s"Failed to get double value for field ${property} in file ${fileName} - value ${sv.get}")
      }

    return Some(doubleValues)
  }
}
