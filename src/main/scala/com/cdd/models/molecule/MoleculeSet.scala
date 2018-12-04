package com.cdd.models.molecule

import java.io.File

import com.cdd.models.datatable.{DataTable, DataTableColumn, SparseVector, VectorBase}
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.utils.{HasLogging, Util}

import scala.io.Source

object MoleculeSet {
  lazy val nciBackground = new MoleculeSet("nci", Util.getProjectFilePath("data/reference/NCI-Open_2012-05-01.smi.gz"))
  lazy val cmcBackground = new MoleculeSet("cmc", Util.getProjectFilePath("data/reference/cmc.smi.gz"))
}

class MoleculeSet(val name:String, val file: File, val fingerprintType: RdkitFingerprintClass = RdkitFingerprintClass.FCFP6)
  extends HasLogging{

  val (ids:Vector[String], fingerprints:Vector[SparseVector]) = build()

  def build() = {
    val fingerprinter = new RdkitDeMorganFingerprinter(fingerprintType)
    logger.info(s"building background dataset for $name")

    Util.using(Util.openFileAsStreamByName(file)) { fin =>
      val in = Source.fromInputStream(fin)
      val output = in.getLines().zipWithIndex.flatMap { case (line, index) =>
        val terms = line.split(" ")
        require(terms.length == 2)
        if (index % 2000 == 0)
          logger.info(s"build $index fingerprints")
        val (smi, name) = (terms(0), terms(1))
        RdkitUtils.smilesToMol(smi) match {
          case Some(mol) =>
            val fingerprint = fingerprinter.fingerprint(mol)
            Some((name, RdkitDeMorganFingerprinter.fingerprintToSparse(fingerprint)))
          case _ => None
        }
      }.toVector.unzip
      logger.info("Loaded all fingerprints")
      output
    }
  }

  def compareDataset(dataSet: Vector[VectorBase]): Vector[Double] = {
    fingerprints.zipWithIndex.map { case (fp, index) =>
      if (index % 2000 == 0)
          logger.info(s"compared $index fingerprints")
      dataSet.par.map(fp.tanimoto).max
    }
  }

  def compareDataTable(dataTable: DataTable): Vector[Double] = {
     fingerprintType match {
      case RdkitFingerprintClass.FCFP6 =>
        val column =
          if (dataTable.hasColumnWithName("rdkit_fp")) {
            dataTable.column("rdkit_fp")
          } else if (dataTable.hasColumnWithName("fingerprints_RDKit_FCFP6")) {
            dataTable.column("fingerprints_RDKit_FCFP6")
          } else {
            throw new IllegalArgumentException
          }
        val dataTableFingerprints = column.asInstanceOf[DataTableColumn[SparseVector]].values.flatten
        logger.info(s"data table comparison with ${dataTableFingerprints.size} fingerprints")
        compareDataset(dataTableFingerprints)
      case _ => throw new IllegalArgumentException
    }
  }



}
