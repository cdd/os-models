package com.cdd.models.datatable

import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.{RdkitDeMorganFingerprinter, RdkitDescriptors}
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.utils.HasLogging
import org.RDKit.{ROMol, RWMol}
import org.apache.log4j.LogManager

import scala.collection.mutable
import scala.reflect.runtime.universe._

class RdkitTableBuilder extends RdkitTableBuilderFunctions with HasLogging {

  def dataFrameFromMolecules(molecules: Vector[Option[ROMol]]): Unit = {
    val columnNos = (0 until molecules.length).map(Some(_)).toVector
    val smiles = molecules.map {
      case Some(m) =>
        val smi = m.MolToSmiles(true)
        Some(smi)
      case _ => None
    }
    val columns = Vector(new DataTableColumn[Int]("no", columnNos),
      new DataTableColumn[String]("rdkit_smiles", smiles))

    logger.warn("Creating datatable")
    dt = Some(new DataTable(columns))
  }

  def setDataTable(df: DataTable):this.type = {
    var dt = df
    require(dt.hasColumnWithName("rdkit_smiles"))
    if (!dt.hasColumnWithName("no")) {
      dt = dt.addColumn(DataTableColumn.fromVector("no", (0 until dt.length).toVector))
    }
    this.dt = Some(dt)
    this
  }
}

trait RdkitTableBuilderFunctions extends TableBuilder with HasLogging {

  protected def addColumnFromSmiles[T: TypeTag](name: String, func: String => Option[T]): this.type = {
    val values = dt.get.column("rdkit_smiles").values.zipWithIndex.map { case (s, i) =>
      if (i % 500 == 0 && i > 0) {
        logger.info(s"addColumnFromSmiles: processing row $i")
      }
      s match {
        case Some(s) => func(s.asInstanceOf[String])
        case None => None
      }
    }
    this.dt = Some(dt.get.addColumn(new DataTableColumn[T](name, values)))
    this
  }


  def addFingerprintColumn(fingerprintClass: RdkitFingerprintClass): this.type = {
    val func: (RdkitFingerprintClass, String) => Option[VectorBase] = (fingerprintClass, smiles) => {
      smiles match {
        case null => None
        case "" => None
        case smiles =>
          RWMol.MolFromSmiles(smiles) match {
            case null =>
              logger.warn(s"Failed to convert smiles $smiles to RKDIT molecule")
              None
            case molecule =>
              val fingerprinter = new RdkitDeMorganFingerprinter(fingerprintClass)
              val fp = fingerprinter.fingerprint(molecule)
              val sparseFp = RdkitDeMorganFingerprinter.fingerprintToSparse(fp)
              Some(sparseFp)
          }
      }
    }

    logger.info(s"Adding Rdkit fingerprints class $fingerprintClass")
    val title = fingerprintClass match {
      case RdkitFingerprintClass.FCFP6 => "fingerprints_RDKit_FCFP6"
      case RdkitFingerprintClass.ECFP6 => "fingerprints_RDKit_ECFP6"
      case _ => throw new IllegalArgumentException
    }
    addColumnFromSmiles(title, func(fingerprintClass, _: String))
    logger.info("Finished adding Rdkit fingerprints")
    this
  }

  def addDescriptorColumns(): this.type = {
    val func: (String => Option[VectorBase]) = (smiles) => {
      smiles match {
        case null => None
        case "" => None
        case smiles =>
          RWMol.MolFromSmiles(smiles) match {
            case null =>
              logger.warn(s"Failed to convert smiles $smiles to RKDIT molecule")
              None
            case molecule =>
              val propertyCalculator = new RdkitDescriptors
              val mlVector = propertyCalculator.physicalProperties(molecule)
              mlVector.map(v => Vectors.Dense(v.toArray))
          }
      }
    }

    logger.info("Adding Rdkit descriptors")
    addColumnFromSmiles("rdkit_descriptors", func)
    logger.info("Finished adding Rdkit descriptors")
    this
  }
}
