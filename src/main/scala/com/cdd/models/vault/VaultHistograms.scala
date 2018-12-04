package com.cdd.models.vault

import java.io.File

import com.cdd.models.universalmetric.NormalizedHistogram
import com.cdd.models.utils.{HasLogging, Util}

import scala.collection.mutable.ArrayBuffer

class VaultDataTableIterator(val filter: VaultDataTable => Boolean = _.dataTable.length >= 100)
  extends Iterator[VaultDataTable] with HasLogging {
  private val vaultFiles = vaultDataTableFiles()
  private var vaultFileNo = 0
  private var vaultDataTableNo = 0
  private var currentDataTables: Vector[VaultDataTable] = _
  private var currentDataTable: VaultDataTable = _

  override def hasNext: Boolean = {
    logger.info("Calling Has Next")
    if (currentDataTable == null)
      nextDataTable()
    currentDataTable != null
  }

  override def next(): VaultDataTable = {
    logger.info("Calling next")
    val rtn = currentDataTable
    currentDataTable = null
    rtn
  }

  def nextDataTable(): Unit = {
    currentDataTable = null
    while (true) {
      if (currentDataTables == null) {
        if (vaultFileNo == vaultFiles.length)
          return
        val vaultFileName = vaultFiles(vaultFileNo).getAbsolutePath
        logger.info(s"reading datafile $vaultFileNo name $vaultFileName")
        currentDataTables = Util.deserializeObject[Vector[VaultDataTable]](vaultFileName)
      }
      for (no <- vaultDataTableNo until currentDataTables.size) {
        logger.info(s"checking datafile $vaultFileNo data table $no of ${currentDataTables.length}")
        val testVaultDataTable = currentDataTables(no)
        if (filter(testVaultDataTable)) {
          vaultDataTableNo = no + 1
          currentDataTable = testVaultDataTable
          logger.info("got datatable")
          return
        }
      }
      currentDataTables = null
      vaultDataTableNo = 0
      vaultFileNo += 1
    }
  }

  private def vaultDataTableFiles() = {
    val dir = Util.getProjectFilePath("data/vault/protocol_molecule_features")
    dir
      .listFiles()
      .filter { d =>
        d.isDirectory
      }
      .flatMap { d =>
        val vaultFile = new File(d, "vaultDataTables.obj")
        if (vaultFile.exists())
          Some(vaultFile)
        else
          None
      }
      .toVector
  }

  def retrieve(nItems: Int): Vector[VaultDataTable] = {
    val items = ArrayBuffer[VaultDataTable]()
    for (_ <- 1 to nItems) {
      if (hasNext)
        items.append(next())
    }
    items.toVector
  }
}

class VaultHistogramIterator extends Iterator[NormalizedHistogram] {
  private val vdtIter = new VaultDataTableIterator(vdt =>
      vdt.dataTable.length >= 100 &&
        vdt.dataTable.column("label").toDoubles().distinct.length > 90)

  override def hasNext: Boolean = vdtIter.hasNext

  override def next(): NormalizedHistogram = vdtIter.next().toHistogram()
}