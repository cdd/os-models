package com.cdd.models.molecule

import java.io.{Closeable, File, FileInputStream, InputStream}

import com.cdd.models.utils.{HasLogging, Util}
import org.apache.log4j.Logger
import org.openscience.cdk.DefaultChemObjectBuilder
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.io.iterator.IteratingSDFReader

object CdkMoleculeReader extends HasLogging {

  def readMolecules(path: String, resource: Boolean = false): List[Option[IAtomContainer]] = {
    Util.using(new CdkMoleculeReader(Util.openPathAsStreamByName(path, resource))) { r =>
      val molecules = r.toList
      return molecules
    }
    throw new RuntimeException(s"Failed to read molecules from ${resource}")
  }

  def extractProperties(fieldName: String, molecules: Iterable[Option[IAtomContainer]]): List[Option[String]] = {
    molecules.map {
      case Some(m) => extractProperty(fieldName, m)
      case None => None
    } toList
  }

  def extractProperty(fieldName: String, molecule: IAtomContainer): Option[String] = {
    val map = molecule.getProperties()
    if (!map.containsKey(fieldName)) {
      logger.debug(s"Unable to find property $fieldName in molecule")
      None
    } else {
      Some(map.get(fieldName).toString)
    }
  }
}

/**
  * Created by gareth on 5/15/17.
  */
class CdkMoleculeReader(inStream: InputStream) extends Iterator[Option[IAtomContainer]] with Closeable {
  val reader = new IteratingSDFReader(inStream, DefaultChemObjectBuilder.getInstance)

  override def hasNext: Boolean = {
    reader.hasNext
  }

  // This return an option to a molecule so in the future we can account for read failures
  override def next(): Option[IAtomContainer] = {
    Some(reader.next())
  }

  override def close() = {
    reader.close
  }
}
