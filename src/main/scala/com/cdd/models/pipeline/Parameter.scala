package com.cdd.models.pipeline

import java.lang.reflect.Modifier
import java.util.UUID

import scala.collection.mutable

trait Identifiable {

  val uid: String

  override def toString: String = uid
}

object Identifiable {

  /**
    * Returns a random UID that concatenates the given prefix, "_", and 12 random hex chars.
    */
  def randomUID(prefix: String): String = {
    prefix + "_" + UUID.randomUUID().toString.takeRight(12)
  }
}

@SerialVersionUID(1000L)
class Parameter[A](val parent: String, val name: String, val description: String = "") extends Serializable {
  def this(parent: Identifiable, name: String, description: String) = this(parent.uid, name, description)
}

trait HasParameters extends Identifiable with Serializable {

  lazy val parameters: Vector[Parameter[_]] = {
    val methods = this.getClass.getMethods.toVector
    val params = methods.filter { m =>
      Modifier.isPublic(m.getModifiers) &&
        classOf[Parameter[_]].isAssignableFrom(m.getReturnType) &&
        m.getParameterTypes.isEmpty
    }.sortBy(_.getName)
      .map(m => m.invoke(this).asInstanceOf[Parameter[_]])
    params
  }

  private val parameterValues = new ParameterMap()

  def setParameter[A](parameter: Parameter[A], value: A): this.type = {
    ownsParameter(parameter)
    parameterValues.put(parameter, value)
    this
  }

  protected def setDefaultParameter[A](parameter: Parameter[A], value: A): this.type = {
    // to be called when setting initial values in constructors- at that point we can't check ownership
    parameterValues.put(parameter, value)
    this
  }


  /**
    * Creates a copy of this instance with the same UID and some extra params.
    * Subclasses should implement this method and set the return type properly.
    * See `defaultCopy()`.
    */
  def copy(extra: ParameterMap): this.type = {
    defaultCopy(extra)
  }

  /**
    * Default implementation of copy with extra params.
    * It tries to create a new instance with the same UID.
    * Then it copies the embedded and extra parameters over and returns the new instance.
    */
  protected final def defaultCopy[T <: HasParameters](extra: ParameterMap): T = {
    val that = this.getClass.getConstructor(classOf[String]).newInstance(uid)
    copyParameterValues(that, extra).asInstanceOf[T]
  }

  protected def copyParameterValues[T <: HasParameters](to: T, extra: ParameterMap = new ParameterMap()): T = {
    val map = parameterValues ++ extra
    parameters.foreach { parameter =>
      if (map.contains(parameter) && to.hasParameter(parameter.name)) {
        to.setParameter(parameter.name, map.get(parameter).get)
      }
    }
    to
  }

  def parameterInfo(): String = {
    parameterValues.toString
  }

  protected def setParameter(parameterName: String, value: Any): this.type = {
    setParameter(getParameterByName(parameterName), value)
    this
  }

  def getParameter[T](parameter: Parameter[T]): T = {
    ownsParameter(parameter)
    val p = parameterValues.get(parameter)
    p match {
      case Some(parameter) => parameter
      case _ => throw new IllegalArgumentException(s"unable to get value for parameter ${parameter.name}")
    }
  }

  def hasParameter(parameterName: String): Boolean = {
    parameters.exists(_.name == parameterName)
  }

  def getParameterByName(parameterName: String): Parameter[Any] = {
    parameters.find(_.name == parameterName).getOrElse {
      throw new NoSuchElementException(s"Param $parameterName does not exist.")
    }.asInstanceOf[Parameter[Any]]
  }

  private def ownsParameter(parameter: Parameter[_]): Unit = {
    require(parameter.parent == uid && hasParameter(parameter.name), s"Param $parameter does not belong to $this.")
  }
}

trait PipelineStage extends HasParameters

@SerialVersionUID(1000L)
class ParameterMap(private val map: mutable.Map[Parameter[Any], Any]) extends Serializable {

  def this() = this(mutable.Map.empty)

  def put[A](parameter: Parameter[A], value: A): this.type = {
    map += (parameter.asInstanceOf[Parameter[Any]] -> value.asInstanceOf[Any])
    this
  }

  def get[T](parameter: Parameter[T]): Option[T] = {
    map.get(parameter.asInstanceOf[Parameter[Any]]).asInstanceOf[Option[T]]
  }

  def contains(parameter: Parameter[_]): Boolean = {
    map.contains(parameter.asInstanceOf[Parameter[Any]])
  }

  def copy: ParameterMap = new ParameterMap(map.clone())

  override def toString: String = {
    map.toSeq.sortBy(_._1.name).map { case (parameter, value) =>
      s"\t${parameter.parent}-${parameter.name}: $value"
    }.mkString("{\n", ",\n", "\n}")
  }

  /**
    * Returns a new param map that contains parameters in this map and the given map,
    * where the latter overwrites this if there exist conflicts.
    */
  def ++(other: ParameterMap): ParameterMap = {
    new ParameterMap(this.map ++ other.map)
  }

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case pm:ParameterMap => map == pm.map
      case _ => false
    }
  }

}



