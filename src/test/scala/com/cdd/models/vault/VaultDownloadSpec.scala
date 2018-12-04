package com.cdd.models.vault

import org.scalatest.{FunSpec, Matchers}

import VaultDownload._
import scala.collection.JavaConverters._

class VaultDownloadSpec extends FunSpec with Matchers{

  describe("Accessing data from the Spark Vault") {

    val vaultId = 4724 // Spark Vault
    val datasets = downloadDatasets(vaultId)
     it ("should have 123 datasets") {
       datasets.length should be(123)

     }

    val test = datasets.find(_.name.contains("Johannes"))
    it ("should not have Johannes data") {
      test shouldBe 'isEmpty
    }
    val projects = downloadProjects(vaultId)
    it ("should have 2 projects") {
      projects.length should be(2)
    }

    val datasetName = "PARASITES: GSK Kineto Box hits with pIC50"
    val datasetOption = datasets.find(_.name == datasetName)

    it (s"should have a dataset with name $datasetName") {
      datasetOption shouldBe 'isDefined
    }

    val dataset = datasetOption.get
    it (s"should have a name of $datasetName") {
      dataset.name should be(datasetName)
      dataset.id should be(4487)
    }

    val datasetId = 4487
    val protocols = downloadProtocolsForDataset(vaultId, datasetId)

    it (s"should have a single protocol for the dataset") {
      protocols.length should be(1)
    }

    val protocol = protocols(0)
    val runIds = protocol.getRuns.map(_.id).toVector
    val protocolData = downloadProtocolData(vaultId, protocol, Some(runIds), Some(datasetId))

    it (s"should have 7235 data items") {
      protocolData.length should be(7235)
    }

    val moleculeIds = protocolData.map( _.protocolData.molecule).distinct
    it (s"should have 592 unique molecule ids") {
      moleculeIds.length should be(592)
    }
    val molecules = downloadMolecules(vaultId, moleculeIds, Some(datasetId))
    it (s"should have downloaded 592 molecule") {
      molecules.length should be(592)
    }


    info("Done")
  }
}
