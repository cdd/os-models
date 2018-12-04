package com.cdd.models.vault

import com.cdd.models.datatable.{DataTable, DataTableColumn}
import com.cdd.models.molecule.MoleculeSet
import com.cdd.models.utils.HasLogging

import scala.collection.mutable.ArrayBuffer


object MoleculeSetProfile extends HasLogging {

  def createSummaryFromVault(moleculeSet: MoleculeSet, data: Vector[DownloadFromVault], names: Vector[String] = null): Vector[DataTableColumn[_]] = {
    val names_ = names match {
      case n: Vector[String] => n
      case _ => data.map(d => s"${d.readoutName}_${d.protocolId}")
    }
    require(names_.length == data.length)

    val dataTables = data.map { d =>
      logger.info("Downloading $d")
      d.downloadFile()
    }
    createSummaryFromDataTables(moleculeSet, dataTables, names_)
  }

  def createSummaryFromDataTables(moleculeSet: MoleculeSet, data: Vector[DataTable], names: Vector[String]): Vector[DataTableColumn[_]] = {
    val similarities = data.map { dt =>
      logger.info(s"Comparing with data table of size ${dt.length}")
      moleculeSet.compareDataTable(dt)
    }

    similarities.zip(names).map { case (s, n) =>
      DataTableColumn.fromVector(n, s)
    }
  }
}
