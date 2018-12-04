package com.cdd.models.datatable

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkMoleculeReader
import com.cdd.models.utils.{HasLogging, SdfileActivityField}
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator}

class CdkInputBuilder(path: String, activityFields: Vector[SdfileActivityField], resource: Boolean, addActivities: Boolean,
                      nameField:Option[String])
  extends InputBuilder[CdkInputBuilder, IAtomContainer](path, activityFields, resource, addActivities, "cdk", nameField)
    with CdkTableBuilderFunctions {

  override def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        try {
          val smiGen = new SmilesGenerator(SmiFlavor.Absolute)
          var smiles = smiGen.create(m)
          if (smiles.isEmpty)
            smiles = null
          val name = nameField.map { f =>
            val n = activityFieldValue(moleculeNo, f)
            require(n.isDefined)
            n.get
          }
          Some(MoleculeInfo(moleculeNo, smiles, name))
        } catch {
          case ex:Exception =>
            logger.warn(s"Error parsing smiles $ex")
            None
        }
      case None => None
    }
  }

  override protected def activityFieldValue(moleculeNo: Int, field: String): Option[String] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        CdkMoleculeReader.extractProperty(field, m)
      case None => None
    }
  }

  override def loadMolecules() = {
    CdkMoleculeReader.readMolecules(path, resource)
  }

  override def addFingerprintColumns(): this.type = {
    addFingerprintColumn(CdkFingerprintClass.ECFP6)
    addFingerprintColumn(CdkFingerprintClass.FCFP6)
    this
  }


}
