package com.cdd.models.datatable

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass.CdkFingerprintClass
import com.cdd.models.molecule.{CdkCircularFingerprinter, CdkDescriptors}
import com.cdd.models.utils.HasLogging
import org.openscience.cdk.exception.InvalidSmilesException
import org.openscience.cdk.fingerprint.CircularFingerprinter
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.silent.SilentChemObjectBuilder
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator, SmilesParser}

import scala.collection.mutable
import scala.reflect.runtime.universe._

class CdkTableBuilder extends CdkTableBuilderFunctions with HasLogging {

  def dataFrameFromMolecules(molecules: Vector[Option[IAtomContainer]]): Unit = {
    val columnNos = (0 until molecules.length).map(Some(_)).toVector
    val smiGen = new SmilesGenerator(SmiFlavor.Absolute)
    val smiles = molecules.map {
      case Some(m) =>
        val smi = smiGen.create(m)
        Some(smi)
      case _ => None
    }
    val columns = Vector(new DataTableColumn[Int]("no", columnNos),
      new DataTableColumn[String]("cdk_smiles", smiles))

    logger.warn("Creating datatable")
    dt = Some(new DataTable(columns))
  }

  def setDataTable(df: DataTable): this.type = {
    var dt = df
    require(dt.hasColumnWithName("cdk_smiles"))
    if (!dt.hasColumnWithName("no")) {
      dt = dt.addColumn(DataTableColumn.fromVector("no", (0 until dt.length).toVector))
    }
    this.dt = Some(dt)
    this
  }
}

object CdkTableBuilderFunctions extends HasLogging {

  def fingerprintMolecule(molecule: IAtomContainer, circ: CircularFingerprinter): SparseVector = {
    circ.calculate(molecule)
    val counts = mutable.Map.empty[Int, Double].withDefaultValue(.0)
    (0 until circ.getFPCount).map {
      circ.getFP(_)
    }.foreach { fp =>
      fp.hashCode match {
        case x if x >= 0 => counts(x) += 1.0
        case x =>
          logger.debug("Negative fingerprint hash- folding")
          counts(-x) += 1.0
      }
    }
    val (index, values) = counts.toArray.sortBy(_._1).unzip
    Vectors.Sparse(index, values, Int.MaxValue)

  }
}

trait CdkTableBuilderFunctions extends TableBuilder with HasLogging {
  protected def addColumnFromSmiles[T: TypeTag](name: String, func: String => Option[T]): this.type = {
    val values = dt.get.column("cdk_smiles").values.map {
      case Some(s) => func(s.asInstanceOf[String])
      case None => None
    }
    this.dt = Some(dt.get.addColumn(new DataTableColumn[T](name, values)))
    this
  }

  def addFingerprintColumn(fingerprintClass: CdkFingerprintClass): this.type = {

    val func: (CdkFingerprintClass, String) => Option[VectorBase] = (fingerprintClass, smiles) => {
      smiles match {
        case null => None
        case "" => None
        case smiles =>
          val circ = new CircularFingerprinter(CdkCircularFingerprinter.fingerprintClassToCdkClass(fingerprintClass))
          val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
          try {
            val molecule = smilesParser.parseSmiles(smiles)
            Some(CdkTableBuilderFunctions.fingerprintMolecule(molecule, circ))
          } catch {
            case ex: InvalidSmilesException =>
              logger.warn(s"CDK failed to parse smiles $smiles ${ex.getMessage}")
              None
          }
      }
    }

    logger.info(s"Adding CDK fingerprints class $fingerprintClass")
    val title = fingerprintClass match {
      case CdkFingerprintClass.FCFP6 => "fingerprints_CDK_FCFP6"
      case CdkFingerprintClass.ECFP6 => "fingerprints_CDK_ECFP6"
      case _ => throw new IllegalArgumentException
    }
    addColumnFromSmiles(title, func(fingerprintClass, _: String))
    logger.info("Finished adding CDK fingerprints")
    this
  }

  def addDescriptorColumns(): this.type = {
    val func: (String => Option[VectorBase]) = (smiles) => {
      smiles match {
        case null => None
        case "" => None
        case smiles =>
          CdkDescriptors.synchronized {
            val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
            try {
              val molecule = smilesParser.parseSmiles(smiles)
              val propertyCalculator = new CdkDescriptors
              val mlVector = propertyCalculator.descriptors(molecule)
              mlVector.map(v => Vectors.Dense(v.toArray))
            } catch {
              case ex: InvalidSmilesException =>
                logger.warn(s"CDK failed to parse smiles $smiles ${ex.getMessage}")
                None
            }
          }
      }
    }

    addColumnFromSmiles("cdk_descriptors", func)
    this
  }

}
