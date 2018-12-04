package com.cdd.models.datatable

import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.molecule.{RdkitDeMorganFingerprinter, RdkitDescriptors, RdkitMoleculeReader}
import com.cdd.models.utils.{SdfileActivityField, TestSdfileProperties}
import org.RDKit.{ROMol, RWMol}

import scala.collection.mutable
import scala.reflect.runtime.universe._


class RdkitInputBuilder(path: String, activityFields: Vector[SdfileActivityField], resource: Boolean,
                        addActivities: Boolean, nameField:Option[String])
  extends InputBuilder[RdkitInputBuilder, ROMol](path, activityFields, resource, addActivities, "rdkit", nameField)
    with RdkitTableBuilderFunctions {


  override def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        var smiles = m.MolToSmiles(true)
        if (smiles.isEmpty)
          smiles = null
        val name = nameField.map { f =>
          val n = activityFieldValue(moleculeNo, f)
          require(n.isDefined)
          n.get
        }
        Some(MoleculeInfo(moleculeNo, smiles, name))
      case None => None
    }
  }

  override def loadMolecules() = {
    logger.info("Loading molecules")
    RdkitMoleculeReader.readMolecules(path, resource)
  }

  override protected def activityFieldValue(moleculeNo: Int, field: String): Option[String] = {
    molecules(moleculeNo).flatMap(m => RdkitMoleculeReader.extractProperty(field, m))
  }

  override def addFingerprintColumns(): this.type = {

    addFingerprintColumn(RdkitFingerprintClass.ECFP6)
    addFingerprintColumn(RdkitFingerprintClass.FCFP6)
    this
  }

}


