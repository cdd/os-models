package com.cdd.models.molecule

import org.openscience.cdk.interfaces.IAtomContainer
import org.scalatest.{FlatSpec, FunSpec, Matchers}

/**
  * Created by gareth on 5/15/17.
  */
class CdkCircularFingerprinterSpec  extends FunSpec with Matchers {

  describe("Fingerprinting molecules using the CDK") {
    val molecules = CdkMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")
    it("should have loaded some molecules") {
      molecules.length should be(743)
    }

    describe("Creating a fingerprint set for the molecules") {
      val moleculeFingerprints = CdkCircularFingerprinter.fingerprintMolecules(molecules)
      it ("should have fingerprinted some molecules") {
        moleculeFingerprints.size should be(743)
      }

      val fingerprintSet = new FingerprintDataset(moleculeFingerprints)
      it ("should have 3968 unique fingerprints") {
        fingerprintSet.uniqueHashesSet.size should be(3968)
      }

      describe("Folding fingerprints") {
        val foldedFingerprints = fingerprintSet.toFolded(1024)
        it ("should have 743 fingerprints") {
          foldedFingerprints.size should be (743)
        }
        it ("each fingerprint should be of length 1024") {
          foldedFingerprints(0).get.size should be (1024)
        }
      }

      describe("Sparse fingerprints") {
        var sparseFingerprints = fingerprintSet.toSparse()
        it ("should have 743 fingerprints") {
          sparseFingerprints.size should be (743)
        }
        it ("each fingerprint should be of length 3968") {
          sparseFingerprints(0).get.size should be (3968)
        }
      }

      info("Tested fingerprints")
    }
  }

}
