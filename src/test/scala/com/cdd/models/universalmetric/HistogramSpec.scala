package com.cdd.models.universalmetric

import java.io.File

import com.cdd.models.utils.Util
import com.cdd.models.vault.{VaultDataTable, VaultDataTableIterator}
import org.scalatest.{FunSpec, Matchers}

class HistogramSpec extends FunSpec with Matchers {
  describe("Creating a histogram") {
    val vaultDataTablesFile = new File("data/vault/protocol_molecule_features/v_1_p_4213_5-HT2C/vaultDataTables.obj")
    val iter = new VaultDataTableIterator(vdt =>
      vdt.dataTable.length >= 100 &&
        vdt.dataTable.column("label").toDoubles().distinct.length > 90)
    val vdts = iter.retrieve(100)
    it ("should have 100 datatables") {
      vdts.length should be(100)
    }
    val histograms = vdts.map(_.toHistogram())
    it ("should have 100 histograms") {
      histograms.length should be(100)
    }

    val plots = histograms.map(_.plot())
    Util.gridPlot("/tmp/histogram_plot.png", plots, 4)
    info("All done!")
  }
}
