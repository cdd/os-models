package com.cdd.models.molecule

import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.utils.{LoadsRdkit, Util}
import org.RDKit.RWMol
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 5/31/17.
  */
class RdkitDeMorganFingerprinterSpec extends FunSpec with Matchers with LoadsRdkit{

  describe("Fingerpringing a molecule") {

    val mol = RWMol.MolFromSmiles("Brc1cc(cc(c1O)Br)C=C(c2ccc(cc2)I)C(=O)O")
    val fingerprinter = new RdkitDeMorganFingerprinter(RdkitFingerprintClass.ECFP6)
    val fingerprint = fingerprinter.fingerprint(mol)

    it("should have created a fingerprint") {
      fingerprint.size should be(44)
      fingerprint.values.max should be(6)
    }
  }

  describe("Fingerpringing a molecule using features") {

    val mol = RWMol.MolFromSmiles("Brc1cc(cc(c1O)Br)C=C(c2ccc(cc2)I)C(=O)O")
    val fingerprinter = new RdkitDeMorganFingerprinter(RdkitFingerprintClass.FCFP6)
    val fingerprint = fingerprinter.fingerprint(mol)

    it("should have created a fingerprint") {
      fingerprint.size should be(39)
      fingerprint.values.max should be(12)
    }
  }

  describe("Fingerprinting molecules using RDKit") {
    val molecules = RdkitMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")

    it("should have loaded some molecules") {
      molecules.length should be(743)
    }

    describe("Creating a ECFP fingerprint set for the molecules") {
      val moleculeFingerprints = RdkitDeMorganFingerprinter.fingerprintMolecules(molecules)
      it("should have fingerprinted some molecules") {
        moleculeFingerprints.size should be(743)
      }

      val fingerprintSet = new FingerprintDataset(moleculeFingerprints)
      it("should have 3996 unique fingerprints") {
        fingerprintSet.uniqueHashesSet.size should be(3996)
      }

      describe("Folding fingerprints") {
        val foldedFingerprints = fingerprintSet.toFolded(1024)
        it("should have 743 fingerprints") {
          foldedFingerprints.size should be(743)
        }
        it("each fingerprint should be of length 1024") {
          foldedFingerprints(0).get.size should be(1024)
        }
      }

      describe("Sparse fingerprints") {
        var sparseFingerprints = fingerprintSet.toSparse()
        it("should have 743 fingerprints") {
          sparseFingerprints.size should be(743)
        }
        it("each fingerprint should be of length 3996") {
          sparseFingerprints(0).get.size should be(3996)
        }
      }

      info("Tested ECFP fingerprints")
    }

    describe("Creating a FCFP fingerprint set for the molecules") {
      val moleculeFingerprints = RdkitDeMorganFingerprinter.fingerprintMolecules(molecules, RdkitFingerprintClass.FCFP6)
      it("should have fingerprinted some molecules") {
        moleculeFingerprints.size should be(743)
      }

      val fingerprintSet = new FingerprintDataset(moleculeFingerprints)
      it("should have 2382 unique fingerprints") {
        fingerprintSet.uniqueHashesSet.size should be(2382)
      }

      describe("Folding fingerprints") {
        val foldedFingerprints = fingerprintSet.toFolded(1024)
        it("should have 743 fingerprints") {
          foldedFingerprints.size should be(743)
        }
        it("each fingerprint should be of length 1024") {
          foldedFingerprints(0).get.size should be(1024)
        }
      }

      describe("Sparse fingerprints") {
        var sparseFingerprints = fingerprintSet.toSparse()
        it("should have 743 fingerprints") {
          sparseFingerprints.size should be(743)
        }
        it("each fingerprint should be of length 2382") {
          sparseFingerprints(0).get.size should be(2382)
        }
      }

      info("Tested RCFP fingerprints")
    }
  }

}