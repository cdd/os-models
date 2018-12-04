package com.cdd.models.vault

import com.cdd.models.utils.Util
import org.scalatest.{FunSpec, Matchers}

class ProtocolFeaturesReadoutsToVaultDataTableSpec extends FunSpec with Matchers {

  describe("Loading the PKIS dataset protocol and molecules and converting to data tables") {

    val base = "v_1_d_KINASE: GSK Published Kinase Inhibitor Set (PKIS)_p_Kinase Assay.obj"
    val filePath = Util.getProjectFilePath("data/vault/protocol_molecule_features/" + base)
    val protocolReadouts = Util.deserializeObject[ProtocolFeaturesReadouts](filePath.getAbsolutePath)
    val noMolecules = protocolReadouts.protocolReadouts.size
    it ("should be a dataset of 364 molecules") {
      noMolecules should be(364)
    }
    val allProtocolFeaturesReadouts = protocolReadouts.toGroupedData()
    it ("should find 236 groups") {
      allProtocolFeaturesReadouts.size should be(236)
    }
    val vaultDataTables = protocolReadouts.toDataTables(allProtocolFeaturesReadouts)
    it ("should find 236 data tables") {
      vaultDataTables.size should be(236)
    }

    // most short names are equivalent to full target names in this protocol.  However, for this full name
    // there are 2 short names (the other is PKC-beta2 (splice variant)).
    val test1 = ProtocolFeaturesReadouts.findByGroupReadoutAndValue(allProtocolFeaturesReadouts, Vector(("Target (short name)", "PKC-beta1 (splice variant)")))
    val test2 = ProtocolFeaturesReadouts.findByGroupReadoutAndValue(allProtocolFeaturesReadouts, Vector(("Target (full name)", "Protein kinase C beta")))
    val groupEquals = test1.get.equalSplit(test2.get)
    it ("should not find the two names equivalent") {
      groupEquals should be(false)
    }
  }

}
