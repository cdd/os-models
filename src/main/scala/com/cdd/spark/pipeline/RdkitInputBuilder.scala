package com.cdd.spark.pipeline

import com.cdd.models.molecule.FingerprintTransform.FingerprintTransform
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.molecule._
import org.RDKit.ROMol
import org.apache.spark.ml.linalg.Vector

/**
  * Created by gjones on 6/5/17.
  */
class RdkitInputBuilder(path: String, resource: Boolean=false) extends InputBuilder[RdkitInputBuilder](path, resource)  {
  private var molecules: Option[List[Option[ROMol]]] = None

  private var fingerprintClass:RdkitFingerprintClass = RdkitFingerprintClass.ECFP6

  override def readMolecules(): Unit = {
     val molecules = RdkitMoleculeReader.readMolecules(path, resource)
    this.molecules = Some(molecules)
  }

  override def extractActivityStrings(field: String): List[Option[String]] = {
    RdkitMoleculeReader.extractProperties(field, molecules.get)
  }
  override def addSmilesColumn(): RdkitInputBuilder.this.type = {
    val smiles = molecules.get.map { case Some(m)=> Some(m.MolToSmiles(true)) case _ => None }
     val func: (Int => String) = (no) => {
      smiles(no).getOrElse(null)
    }
    addColumn("rdkit_smiles", func)
  }

  override def addDescriptorsColumn(): RdkitInputBuilder.this.type = {
    val properties = RdkitDescriptors.descriptorsForMolecules(molecules.get)
    val func: (Int => Vector) = (no) => {
      properties(no).getOrElse(null)
    }
   addColumn("rdkit_descriptors", func)
  }

  override def addFingerprintColumn(): RdkitInputBuilder.this.type = {
    val fingerprintMaps = RdkitDeMorganFingerprinter.fingerprintMolecules(molecules.get, fingerprintClass)
    val fingerprintDataset = new FingerprintDataset(fingerprintMaps)
    val fingerprints = fingerprintDataset.toVectors(fingerprintTransform, foldSize)

    val func: (Int => Vector) = (no) => {
      fingerprints(no).getOrElse(null)
    }
    addColumn("fingerprints_"+RdkitDeMorganFingerprinter.fingerprintName(fingerprintClass), func)
  }

  def addFingerprintColumn(fingerprintClass: RdkitFingerprintClass = RdkitFingerprintClass.ECFP6,
                           fingerprintTransform: FingerprintTransform = FingerprintTransform.Fold, foldSize: Int = 1024): this.type = {
    this.fingerprintClass = fingerprintClass
    this.foldSize = foldSize
    this.fingerprintTransform = fingerprintTransform
    addFingerprintColumn()
  }

  def setFingerprintClass(fingerprintClass: RdkitFingerprintClass) : this.type = {
    this.fingerprintClass = fingerprintClass
    this
  }

}
