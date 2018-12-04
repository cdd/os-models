package com.cdd.models.molecule

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass.CdkFingerprintClass
import org.openscience.cdk.fingerprint.CircularFingerprinter
import org.openscience.cdk.interfaces.IAtomContainer

/**
  * Created by gareth on 5/15/17.
  */

object CdkCircularFingerprinter {

  object CdkFingerprintClass extends Enumeration {
    type CdkFingerprintClass = Value
    val ECFP6, FCFP6 = Value
  }

   def fingerprintClassToCdkClass(fingerprintClass: CdkFingerprintClass): Int = {
    fingerprintClass match {
      case CdkFingerprintClass.ECFP6 => CircularFingerprinter.CLASS_ECFP6
      case CdkFingerprintClass.FCFP6 => CircularFingerprinter.CLASS_FCFP6
    }
  }

  def fingerprintName(fingerprintClass: CdkFingerprintClass): String = {
    fingerprintClass match {
      case CdkFingerprintClass.ECFP6 => "CDK_ECFP6"
      case CdkFingerprintClass.FCFP6 => "CDK_FCFP6"
    }
  }

  def fingerprintMolecules(molecules: List[Option[IAtomContainer]], fingerprintType: CdkFingerprintClass = CdkFingerprintClass.ECFP6): List[Option[Map[Long, Int]]] = {
    val fingerprinter = new CdkCircularFingerprinter(fingerprintType)
    molecules.map {
      case Some(molecule) => Some(fingerprinter.fingerprint(molecule))
      case None => None
    }
  }

}

class CdkCircularFingerprinter(fingerprintType: CdkFingerprintClass = CdkFingerprintClass.ECFP6) extends FingerprinterBase{

  /**
    * Fingerprints a molecule. The fingerprint is a map of hashes to counts.  Hashes are converted from signed ints to unsigned longs
    *
    * @param molecule
    * @return
    */
  def fingerprint(molecule: IAtomContainer): Map[Long, Int] = {
    val circ = new CircularFingerprinter(CdkCircularFingerprinter.fingerprintClassToCdkClass(fingerprintType))
    //val countFingerprint = circ.getCountFingerprint(molecule)
    //0 until countFingerprint.numOfPopulatedbins() map (index => Integer.toUnsignedLong(countFingerprint.getHash(index)) -> countFingerprint.getCount(index)) toMap
    circ.calculate(molecule)
    (0 until circ.getFPCount).map { i => circ.getFP(i) }.map { fp => Integer.toUnsignedLong(fp.hashCode) -> 1 } toMap
  }
}
