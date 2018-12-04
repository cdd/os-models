package com.cdd.models.vault

import org.scalatest.{FunSpec, Matchers}
import VaultDownload._
import scala.collection.JavaConverters._

class ToxDataDownloadSpec extends FunSpec with Matchers{

  describe("Finding tox data in the vault") {
    val vaultId = 1 // CDD Vault
    val datasets = downloadDatasets(vaultId)

    val toxDatasets = datasets.filter {
      _.name.toLowerCase.contains("tox")
    }

    print(s"found ${toxDatasets.length} tox datasets")

    it ("should have 8 tox datasets") {
      toxDatasets.length should be(8)
    }

    toxDatasets.foreach { toxDataset =>
      val protocols = downloadProtocolsForDataset(vaultId, toxDataset.id)

      println()
      println(s"Dataset '${toxDataset.name}'")
      protocols.foreach { protocol =>
        val readoutNames = protocol.readoutDefinitions.map(_.name)
        println(s"Protocol '${protocol.name}' readouts:")

        val readoutData = downloadProtocolData(vaultId, protocol, datasetId = Some(toxDataset.id))

        protocol.readoutDefinitions.foreach {rd =>
          val nReadouts = readoutData.count { data => data.readoutValues.exists(_.readoutDefinitionId == rd.id) }
          val desc = if (rd.description != null) s" Desc ${rd.description}" else ""
          println(s"\tReadout '${rd.name}' Type ${rd.dataType}$desc Count $nReadouts")
        }

      }

    }

  }

}
