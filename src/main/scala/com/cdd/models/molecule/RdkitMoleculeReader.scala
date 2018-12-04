package com.cdd.models.molecule

import java.io._
import java.util.UUID

import com.cdd.models.utils.{HasLogging, LoadsRdkit, Util}
import org.RDKit.{ROMol, RWMol, SDMolSupplier}
import org.apache.log4j.Logger

import scala.io.Source
import scalax.file.Path

/**
  * Created by gjones on 5/23/17.
  */

object RdkitMoleculeReader extends LoadsRdkit{
  def readMolecules(path: String, resource: Boolean = false): List[Option[ROMol]] = {
    val reader =
      if (resource) new RdkitMoleculeReader(Util.openResourceAsStreamByName(path))
      else {
        val filePath = Util.getProjectFilePath(path)
        if (filePath.getPath.toLowerCase().endsWith(".gz")) {
          new RdkitMoleculeReader(Util.openGzippedFileAsStream(filePath))
        } else {
          new RdkitMoleculeFileReader(filePath.getPath)
        }
      }
    Util.using(reader) { r =>
      val molecules = r.toList
      return molecules
    }
    throw new RuntimeException(s"Failed to read molecules from ${resource}")
  }

  def extractProperties(fieldName: String, molecules: Iterable[Option[ROMol]]): List[Option[String]] = {
    molecules.map {
      case Some(m) => extractProperty(fieldName, m)
      case _ => None
    } toList
  }

  def extractProperty(fieldName: String, molecule: ROMol): Option[String] = {
    if (!molecule.hasProp(fieldName)) {
      //throw new IllegalArgumentException(s"Unable to find property $fieldName in molecule")
      None
    } else {
      Some(molecule.getProp(fieldName))
    }
  }

}

class RdkitMoleculeReader(inStream: InputStream) extends Iterator[Option[ROMol]] with Closeable {
  val uuid = UUID.randomUUID.toString
  // don't create outfile in tmp, as we may run out of space- use process directory
  //val outFile = new File(System.getProperty("java.io.tmpdir"), s"tmp_sd_${uuid}.sdf")
  val outFile = new File(s"tmp_sd_${uuid}.sdf")
  Path(outFile).write(Source.fromInputStream(inStream, "UTF-8"))
  val reader = new RdkitMoleculeFileReader(outFile.getAbsolutePath)

  override def hasNext: Boolean = {
    reader.hasNext
  }

  override def next(): Option[ROMol] = {
    reader.next()
  }

  override def close() = {
    outFile.delete()
  }
}

class RdkitMoleculeFileReader(fileName: String) extends Iterator[Option[ROMol]] with Closeable with HasLogging {
   val sdSupplier: SDMolSupplier = new SDMolSupplier(fileName, true)

  override def hasNext: Boolean = {
    !sdSupplier.atEnd()
  }

  override def next(): Option[ROMol] = {
    val mol = sdSupplier.next()
    if (mol == null) {
      this.logger.warn("Failed to read molecule from file!")
      None
    } else {
      Some(mol)
    }
  }

  override def close() = {}
}

// I've left this reader in for now, as I may wish to reuse the code, but the MolBlock reader does not read SD fields
class RdkitMoleculeReader2(inStream: InputStream) extends Iterator[ROMol] with Closeable {
  val lines = Source.fromInputStream(inStream, "UTF-8").getLines()
  var molBlock: Option[String] = None

  override def hasNext: Boolean = {
    val text = lines.takeWhile(!_.startsWith("$$$$")).mkString("\n")
    molBlock = if (text.length > 20) Some(text) else None
    molBlock != None
  }

  override def next(): ROMol = {
    RWMol.MolFromMolBlock(molBlock.get)
  }

  override def close() = {
    inStream.close
  }
}
