package com.cdd.models.tox

import com.cdd.models.datatable.{DataTable, DataTableColumn, DataTableModelUtil}
import com.cdd.models.molecule.MoleculeSet
import com.cdd.models.utils.{LoadsRdkit, Util}
import com.cdd.models.vault.{DownloadFromVault, MoleculeSetProfile}

object MoleculeSetComparison {

  val datasets: Vector[DownloadFromVault] = Vector(
    DownloadFromVault.downloadFromVaultDataset(1, "ADME: AZ Public ChEMBL Data", "Human microsomal intrinsic clearance", "AZ Human Microsomal Intrinsic Clearance"),
    DownloadFromVault.downloadFromVaultDataset(1, "ADME: AZ Public ChEMBL Data", "Protein Binding", "AZ Human protein binding"),
    DownloadFromVault.downloadFromVaultDataset(1, "ADME: AZ Public ChEMBL Data", "Protein binding", "AZ Dog protein binding"),
    DownloadFromVault.downloadFromVaultDataset(1, "ADME: AZ Public ChEMBL Data", "Protein binding", "AZ Rat protein binding"),

    DownloadFromVault.downloadFromVaultProject(3663, "G2 Research ADMEdata.com", "Human Protein Binding", "Protein Binding - Human"),
    DownloadFromVault.downloadFromVaultProject(3663, "G2 Research ADMEdata.com", "Rat Protein Binding", "Protein Binding - Rat"),

    DownloadFromVault.downloadFromVaultDataset(1, "MULTIPLE SCLEROSIS: OL Toxicity Screening", "Average at 10 uM", "Oligodendrocyte Toxicity Assay"),
    DownloadFromVault.downloadFromVaultDataset(1, "FDA APPROVED, TOX: Maximum recommended daily dose", "MRDD dose (mM)", "Maximum recommended daily dose")

  )

  val names: Vector[String] = Vector(
    "AZ Human Microsomal Intrinsic Clearance",
    "AZ Human protein binding",
    "AZ Dog protein binding",
    "AZ Rat protein binding",

    "AD Human protein binding",
    "AD Rat protein binding",

    "MS Tox",
    "FDA MRDD"
  )

  val toxAssays = Vector("NR-AhR", "NR-AR-LBD", "SR-MMP", "SR-p53")

  def process(fileName: String, moleculeSet: MoleculeSet): Unit = {
    val vaultColumns = MoleculeSetProfile.createSummaryFromVault(moleculeSet, datasets, names)

    val tox21Dt = Tox21Compounds.buildDf()
    val toxDts = toxAssays.map { assay =>
      DataTableModelUtil.selectTable(tox21Dt, assay)
    }
    val toxColumns = MoleculeSetProfile.createSummaryFromDataTables(moleculeSet, toxDts, toxAssays)

    val idColumn = DataTableColumn.fromVector("ID", moleculeSet.ids)
    val dt = new DataTable(Vector(idColumn) ++ vaultColumns ++ toxColumns)
    dt.exportToFile(fileName)
  }

}

object MoleculeSetComparisonWithCmc extends App with LoadsRdkit {
  val moleculeSet = MoleculeSet.cmcBackground
  val fileName = Util.getProjectFilePath("data/vault/adme_tox_cmc.csv.gz").getAbsolutePath
  MoleculeSetComparison.process(fileName, moleculeSet)
}


object MoleculeSetComparisonWithNci extends App with LoadsRdkit {
  val moleculeSet = MoleculeSet.nciBackground
  val fileName = Util.getProjectFilePath("data/vault/adme_tox_nci.csv.gz").getAbsolutePath
  MoleculeSetComparison.process(fileName, moleculeSet)
}
