package com.cdd.models.pipeline.tuning

import com.cdd.models.pipeline.{Parameter, ParameterMap}

import scala.collection.mutable

case class ParameterGrid(parameters: Vector[Parameter[_]], parameterMap:Vector[ParameterMap])

class ParameterGridBuilder {

  private val paramGrid = mutable.ListMap.empty[Parameter[_], Vector[_]]

  /**
    * Adds a param with multiple values (overwrites if the input param exists).
    */
  def addGrid[T](parameter: Parameter[T], values: Vector[T]): this.type = {
    paramGrid.put(parameter, values)
    this
  }

  /**
   * Builds and returns all combinations of parameters specified by the param grid.
   */
  def build(): ParameterGrid = {
    var parameterMaps = Vector(new ParameterMap())
    paramGrid.foreach { case (param, values) =>
      val newParamMaps = values.flatMap { v =>
        parameterMaps.map(_.copy.put(param.asInstanceOf[Parameter[Any]], v))
      }
      parameterMaps = newParamMaps.toVector
    }
    val expected = paramGrid.values.foldLeft(1.0)((s, g) => s*g.length)
    require(expected==parameterMaps.length)
    new ParameterGrid(paramGrid.keys.toVector, parameterMaps)
  }
}