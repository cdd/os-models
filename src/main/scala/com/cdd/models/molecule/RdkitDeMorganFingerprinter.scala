package com.cdd.models.molecule

import com.cdd.models.datatable.{SparseVector, Vectors}
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.utils.HasLogging
import org.RDKit.{RDKFuncs, ROMol, RWMol}

import scala.collection.mutable

/**
  * Created by gjones on 5/30/17.
  */
object RdkitDeMorganFingerprinter extends HasLogging {

  object RdkitFingerprintClass extends Enumeration {
    type RdkitFingerprintClass = Value
    val ECFP6, FCFP6 = Value
  }

  def fingerprintName(fingerprintClass: RdkitFingerprintClass): String = {
    fingerprintClass match {
      case RdkitFingerprintClass.ECFP6 => "RDKit_ECFP6"
      case RdkitFingerprintClass.FCFP6 => "RDKit_FCFP6"
      case _ => throw new IllegalArgumentException
    }
  }

  def fingerprintSettings(fingerprintClass: RdkitFingerprintClass): (Int, Boolean) = {
    fingerprintClass match {
      case RdkitFingerprintClass.ECFP6 => (3, false)
      case RdkitFingerprintClass.FCFP6 => (3, true)
      case _ => throw new IllegalArgumentException
    }
  }

  def fingerprintMolecules(molecules: List[Option[ROMol]], fingerprintType: RdkitFingerprintClass = RdkitFingerprintClass.ECFP6): List[Option[Map[Long, Int]]] = {
    val fingerprinter = new RdkitDeMorganFingerprinter(fingerprintType)
    molecules.map {
      case Some(molecule) => Some(fingerprinter.fingerprint(molecule))
      case None => None
    }
  }

  def fingerprintToSparse(fp:Map[Long, Int]): SparseVector = {
    val counts = mutable.Map.empty[Int, Double].withDefaultValue(.0)
    fp.foreach { case (index, value) =>
      index match {
        case i if i >= 0 && i <= Int.MaxValue => counts(i.toInt) += value
        case i if i > Int.MaxValue =>
          val intIndex = (i - Int.MaxValue).toInt
          logger.debug(s"Long fingerprint hash- folding ${i} to ${intIndex}")
          counts(intIndex) += value
      }
    }

    val (index, values) = counts.toArray.sortBy(_._1).unzip
    Vectors.Sparse(index, values, Int.MaxValue)
  }
}

class RdkitDeMorganFingerprinter(fingerprintType: RdkitFingerprintClass = RdkitFingerprintClass.ECFP6) extends FingerprinterBase {

  /**
    * Fingerprints a molecule. The fingerprint is a map of hashes to counts.  Hashes are converted from signed ints to unsigned longs
    *
    * @param molecule
    * @return
    */
  def fingerprint(molecule: ROMol): Map[Long, Int] = {
    val (radius, useFeatures) = RdkitDeMorganFingerprinter.fingerprintSettings(fingerprintType)
    val sparseVector = if (useFeatures) {
      RDKFuncs.getFeatureFingerprint(molecule, radius)
    } else {
      /**
        * C ++ signature is parseIntVect<uint32_t> *getFingerprint(const ROMol &mol, unsigned int radius,
        * std::vector<uint32_t> *invariants,
        * const std::vector<uint32_t> *fromAtoms,
        * bool useChirality, bool useBondTypes,
        * bool useCounts,
        * bool onlyNonzeroInvariants,
        * BitInfoMap *atomsSettingBits)
        */
      RDKFuncs.MorganFingerprintMol(molecule, radius, null, null, false, true, true)
    }
    val elements = sparseVector.getNonzero
    (0 until elements.size().toInt).map { i =>
      val el = elements.get(i)
      el.getFirst -> el.getSecond
    }.toMap
  }
}

