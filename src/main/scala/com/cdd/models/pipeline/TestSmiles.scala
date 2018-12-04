package com.cdd.models.pipeline

import com.cdd.models.datatable._
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass.CdkFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.utils.{LoadsRdkit, Util}

import scala.collection.mutable.ArrayBuffer

class TestSmiles(model: PipelineModel) {

  import TestSmiles._

  def this(modelFileName: String) =
    this(PipelineModel.loadPipeline(modelFileName))


  private def fitData(smiArray: Vector[String]): DataTable = {
    val featuresColumn = model.getFeaturesColumn()
    val dataTable = testSmilesDataTable(smiArray, featuresColumn).filterNull()
    model.transform(dataTable)
  }


  def predict(smiArray: Vector[String]): Vector[Option[Double]] = {
    val predictDataTable = fitData(smiArray)
    require(predictDataTable.hasColumnWithName("prediction"))
    val results = ArrayBuffer.fill[Option[Double]](smiArray.size)(None)
    predictDataTable.column("moleculeNo").toInts()
      .zip(predictDataTable.column("prediction").toDoubles())
      .foreach { case (moleculeNo: Int, prediction: Double) =>
        results(moleculeNo - 1) = Some(prediction)
      }
    results.toVector
  }


  def predictWithProbabilities(smiArray: Vector[String]): Vector[Option[(Double, Double)]] = {
    val predictDataTable = fitData(smiArray)
    require(predictDataTable.hasColumnWithName("prediction"))
    require(predictDataTable.hasColumnWithName("probability"))
    val results = ArrayBuffer.fill[Option[(Double, Double)]](smiArray.size)(None)
    predictDataTable.column("moleculeNo").toInts()
      .zip(predictDataTable.column("prediction").toDoubles())
      .zip(predictDataTable.column("probability").toDoubles())
      .foreach { case ((moleculeNo: Int, prediction: Double), probability: Double) =>
        results(moleculeNo - 1) = Some((prediction, probability))
      }
    results.toVector
  }

}

object TestSmiles extends LoadsRdkit {

  def testSmilesDataTable(smiArray: Vector[String], featuresColumn: String): DataTable = {
    featuresColumn match {
      case "fingerprints_RDKit_FCFP6" =>
        rdkitFingerprintTable(smiArray, RdkitFingerprintClass.FCFP6)
      case "rdkit_fp" =>
        rdkitFingerprintTable(smiArray, RdkitFingerprintClass.FCFP6)
          .rename("fingerprints_RDKit_FCFP6" -> "rdkit_fp")
      case "rdkit_descriptors" =>
        rdkitDescriptorTable(smiArray)
      case "rdkit_desc" =>
        rdkitDescriptorTable(smiArray)
          .rename("rdkit_descriptors" -> "rdkit_desc")
      case "fingerprints_CDK_FCFP6" =>
        cdkFingerprintTable(smiArray, CdkFingerprintClass.FCFP6)
      case "cdk_fp" =>
        cdkFingerprintTable(smiArray, CdkFingerprintClass.FCFP6)
          .rename("fingerprints_CDK_FCFP6" -> "cdk_fp")
      case "cdk_descriptors" =>
        cdkDescriptorTable(smiArray)
      case "cdk_desc" =>
        cdkDescriptorTable(smiArray)
          .rename("cdk_descriptors" -> "cdk_desc")
      case s => throw new IllegalArgumentException(s"Unknown features column $s")
    }
  }

  private def smilesArrayToDataTable(smiArray: Vector[String], smilesColumnName: String): DataTable = {
    val moleculeNos = (1 to smiArray.length).toVector
    val moleculeNosColumn = DataTableColumn.fromVector("moleculeNo", moleculeNos)
    val smilesColumn = DataTableColumn.fromVector(smilesColumnName, smiArray)
    new DataTable(Vector(moleculeNosColumn, smilesColumn))
  }

  private def rdkitFingerprintTable(smiArray: Vector[String], fingerprintType: RdkitFingerprintClass): DataTable = {
    val dt = smilesArrayToDataTable(smiArray, "rdkit_smiles")
    new RdkitTableBuilder()
      .setDataTable(dt)
      .addFingerprintColumn(fingerprintType).getDt
  }

  private def rdkitDescriptorTable(smiArray: Vector[String]): DataTable = {
    val dt = smilesArrayToDataTable(smiArray, "rdkit_smiles")
    new RdkitTableBuilder()
      .setDataTable(dt)
      .addDescriptorColumns().getDt
  }

  private def cdkFingerprintTable(smiArray: Vector[String], fingerprintType: CdkFingerprintClass): DataTable = {
    val dt = smilesArrayToDataTable(smiArray, "cdk_smiles")
    new CdkTableBuilder()
      .setDataTable(dt)
      .addFingerprintColumn(fingerprintType).getDt
  }

  private def cdkDescriptorTable(smiArray: Vector[String]): DataTable = {
    val dt = smilesArrayToDataTable(smiArray, "cdk_smiles")
    new CdkTableBuilder()
      .setDataTable(dt)
      .addDescriptorColumns().getDt
  }


  def allDataTable(smiArray: Vector[String], parallel: Boolean = false, skipRdKitDescriptors: Boolean = false): DataTable = {

    val rdkitDt = smilesArrayToDataTable(smiArray, "rdkit_smiles").rename("moleculeNo" -> "no")
    val rdkitFn: DataTable => DataTable = { rdkitDt =>
      val rdkitInputBuilder = new RdkitTableBuilder().setDataTable(rdkitDt)
        .addFingerprintColumn(RdkitFingerprintClass.FCFP6)
      if (!skipRdKitDescriptors)
        rdkitInputBuilder.addDescriptorColumns()
      rdkitInputBuilder.getDt
    }

    // Note CDK does not support thread safe descriptor calculations
    val cdkDt = rdkitDt.rename("rdkit_smiles" -> "cdk_smiles")
    val cdkFn: DataTable => DataTable = { cdkDt =>
      val cdkTableBuilder = new CdkTableBuilder().setDataTable(cdkDt)
        .addFingerprintColumn(CdkFingerprintClass.FCFP6)
      cdkTableBuilder.getDt
    }

    val dt = parallel match {
      case true =>
        val chunkSize = 500
        val rDt = ParallelDataTable.process(rdkitDt, rdkitFn, chunkSize)
        var cDt = ParallelDataTable.process(cdkDt, cdkFn, chunkSize)
        cDt = new CdkTableBuilder().setDataTable(cDt).addDescriptorColumns().getDt
        InputBuilder.mergeCdkAndRdkit(cDt.sort("no", true), rDt.sort("no", true))
      case false =>
        val rDt = rdkitFn(rdkitDt)
        var cDt = cdkFn(cdkDt)
        cDt = new CdkTableBuilder().setDataTable(cDt).addDescriptorColumns().getDt
        InputBuilder.mergeCdkAndRdkit(cDt, rDt)
    }
    var columns = Vector("fingerprints_RDKit_FCFP6" -> "rdkit_fp",
      "fingerprints_CDK_FCFP6" -> "cdk_fp",
      "cdk_descriptors" -> "cdk_desc")
    if (dt.hasColumnWithName("rdkit_descriptors"))
      columns = columns :+ ("rdkit_descriptors" -> "rdkit_desc")
    dt.rename(columns:_*)
  }
}
