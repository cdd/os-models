package com.cdd.models.vault

import org.scalatest.{FunSpec, Matchers}
import VaultDownload._
import com.cdd.models.molecule.MoleculeSet
import com.cdd.models.utils.LoadsRdkit
import org.apache.commons.math3.stat.descriptive.rank.Percentile

class VaultDistanceToMoleculeSetSpec extends FunSpec with Matchers with LoadsRdkit {

  describe("checking CMC self distances") {
    val fingerprints = MoleculeSet.cmcBackground.fingerprints
    val cmcDistances = MoleculeSet.cmcBackground.compareDataset(fingerprints)

    it ("should have CMC to CMC distances of all 1") {
      cmcDistances.foreach { d =>
        d should be(1.0)
      }
    }
  }

  /* this block will take a couple of days to run


  describe("checking nci self distances") {

    val fingerprints = MoleculeSet.nciBackground.fingerprints
    val nciDistances = MoleculeSet.nciBackground.compareDataset(fingerprints)

    it ("should have NCI to NCI distances of all 1") {
      nciDistances.foreach { d =>
        d should be(1.0)
      }
    }
  }

 */

  describe("Loading an AZ dataset and finding closest CMC neighbours") {

    val vaultId = 1
    val dataset = downloadDatasets(vaultId).find(_.name == "ADME: AZ Public ChEMBL Data").get
    val protocolName = "AZ Human protein binding"
    val protocol = downloadProtocolsForDataset(vaultId, dataset.id).find(_.name == protocolName).get
    val readoutName = "Protein Binding"
    val downloader = new DownloadFromVault(vaultId, Some(dataset.id), readoutName, Some(protocol.id))
    val dt = downloader.downloadFile()

    val cmcDistances = MoleculeSet.cmcBackground.compareDataTable(dt)
    val percentile = new Percentile()
    percentile.setData(cmcDistances.toArray)
    val quartile1 = percentile.evaluate(25.0)
    val quartile3 = percentile.evaluate(75.0)
    val mid = percentile.evaluate(50.0)

    it ("should have a correct 1st quartile") {
      quartile1 should be(0.68 +- 0.01)
    }
    it ("should have a correct median") {
      mid should be(0.77 +- 0.01)
    }
    it ("should have a correct 3rd quartile") {
      quartile3 should be(0.83 +- 0.01)
    }
  }

}
