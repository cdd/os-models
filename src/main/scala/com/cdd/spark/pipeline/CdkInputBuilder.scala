package com.cdd.spark.pipeline

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform.FingerprintTransform
import com.cdd.models.molecule._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator}


/**
  * Created by gareth on 5/16/17.
  */
class CdkInputBuilder(path: String, resource: Boolean = false) extends InputBuilder[CdkInputBuilder](path, resource) {

  private var molecules: Option[List[Option[IAtomContainer]]] = None

  private var fingerprintClass = CdkFingerprintClass.ECFP6

  override def readMolecules(): Unit = {
    val molecules = CdkMoleculeReader.readMolecules(path, resource)
    this.molecules = Some(molecules)
  }

  override def addSmilesColumn(): this.type = {
    val smiGen = new SmilesGenerator(SmiFlavor.Absolute)
    val smiles = molecules.get.map { case Some(m) => Some(smiGen.create(m)) case _ => None }
    val func: (Int => String) = (no) => {
      smiles(no).getOrElse(null)
    }
    addColumn("cdk_smiles", func)
  }

  override def extractActivityStrings(field: String): List[Option[String]] = {
    CdkMoleculeReader.extractProperties(field, molecules.get)
  }

  def addFingerprintColumn(fingerprintClass: CdkFingerprintClass = CdkFingerprintClass.ECFP6,
                           fingerprintTransform: FingerprintTransform = FingerprintTransform.Fold, foldSize: Int = 1024): CdkInputBuilder = {
    this.fingerprintClass = fingerprintClass
    this.foldSize = foldSize
    this.fingerprintTransform = fingerprintTransform
    addFingerprintColumn()
  }

  override def addFingerprintColumn(): this.type = {
    val fingerprintMaps = CdkCircularFingerprinter.fingerprintMolecules(molecules.get, fingerprintClass)
    val fingerprintDataset = new FingerprintDataset(fingerprintMaps)
    val fingerprints = fingerprintDataset.toVectors(fingerprintTransform, foldSize)

    val func: (Int => Vector) = (no) => {
      fingerprints(no).getOrElse(null)
    }
    addColumn("fingerprints_" + CdkCircularFingerprinter.fingerprintName(fingerprintClass), func)
  }

  override def addDescriptorsColumn(): this.type = {
    val properties = CdkDescriptors.descriptorsForMolecules(molecules.get)

    val func: (Int => Vector) = (no) => {
      properties(no).getOrElse(null)
    }

    addColumn("cdk_descriptors", func)
  }

  def setFingerprintClass(fingerprintClass: CdkFingerprintClass): this.type = {
    this.fingerprintClass = fingerprintClass
    this
  }

}
