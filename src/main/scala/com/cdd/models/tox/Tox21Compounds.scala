package com.cdd.models.tox

import java.io.{File, FileWriter}

import com.cdd.models.datatable.{DataTable, DataTableColumn, SdfileReader}
import com.cdd.models.utils.{HasLogging, TestDirectoryLocalProperties, Util}
import com.opencsv.CSVWriter
import com.univocity.parsers.csv.CsvWriter

object Tox21Compounds extends HasLogging {
  val directory = Util.getProjectFilePath("data/Tox21")
  val dfFile = new File(directory, "tox21_10k_data_all.csv.gz")
  val testDfFile = new File(directory, "tox21_10k_challenge_test.csv.gz")
  val scoreDfFile = new File(directory, "tox21_10k_challenge_score_dt.csv.gz")

  def buildDf(): DataTable = {
    if (!dfFile.exists()) {
      val localProperties = new TestDirectoryLocalProperties(directory)
      localProperties.processFile("tox21_10k_data_all.sdf")
    }
    DataTable.loadProjectFile(dfFile.getAbsolutePath).select(Tox21ClassificationModel.selectColumns:_*)
  }

  def buildTestAndScoreDf(): (DataTable, DataTable) = {
    val localProperties = new TestDirectoryLocalProperties(directory)
    if (!testDfFile.exists()) {
      localProperties.processFile("tox21_10k_challenge_test.sdf")
    }
    if (!scoreDfFile.exists()) {
      val sdFile = new File(directory, "tox21_10k_challenge_score.sdf")
      val csvFile = new File(directory, "tox21_10k_challenge_score.csv")
      val scoreSdDataTable = SdfileReader.sdFileToDataTable(sdFile.getAbsolutePath, Vector.empty, nameField = Some("Sample ID"))
      val activityDataTable = DataTable.loadProjectFile(csvFile.getAbsolutePath, separator = '\t')

      val newColumns = activityDataTable.columns.map { c =>
        c.title match {
          case "Sample ID" => c
          case t =>
            val newValues = c.values.asInstanceOf[Vector[Option[String]]].map {
              case Some("x") => None
              case Some("1") => Some(1.0)
              case Some("0") => Some(0.0)
              case _ => throw new IllegalArgumentException
            }
            new DataTableColumn[Double](t, newValues, classOf[Double])
        }
      }

      val scoreDf = new DataTable(scoreSdDataTable.columns ++ newColumns)
      for (row <- scoreDf.rows()) {
        val sample1 = row("Sample ID").asInstanceOf[Option[String]]
        val sample2 = row("cdk_Sample ID").asInstanceOf[Option[String]]
        val sample3 = row("rdkit_Sample ID").asInstanceOf[Option[String]]

        if (sample1.isDefined && sample2.isDefined)
          require(sample1.get == sample2.get)
        if (sample1.isDefined && sample3.isDefined)
          require(sample1.get == sample3.get)
      }
      scoreDf.exportToFile(scoreDfFile.getAbsolutePath)
    }

    val testDf = DataTable.loadProjectFile(testDfFile.getAbsolutePath)
    val scoreDf = DataTable.loadProjectFile(scoreDfFile.getAbsolutePath)
    (testDf.select(Tox21ClassificationModel.selectColumns:_*), scoreDf.select(Tox21ClassificationModel.selectColumns:_*))
  }

  def main(args: Array[String]): Unit = {
    // kept in for compatibility - use com.cdd.models.tox.Tox21ClassificationModel
    Tox21ClassificationModel.applyToxFields()
  }
}

object Tox21Hitrate extends HasLogging with App {
  Util.using(new FileWriter(Util.getProjectFilePath("data/Tox21/tox21_10k_data_hitrate.csv"))) { fh =>
    val csvWriter = new CSVWriter(fh)
    csvWriter.writeNext(Array("toxField", "noCompounds", "noActive", "hitrate"))
    for {
      toxAssay <- Tox21ClassificationModel.toxAssays
    } {
      val dt = Tox21ClassificationModel.inputTable(toxAssay)
      val labels = dt.column("label").toDoubles()
      val nCompounds = dt.length
      assert(labels.length == nCompounds)
      val nActive = labels.count(_ == 1.0)
      val hitrate = nActive.toDouble / nCompounds.toDouble
      println(s"$toxAssay: No compounds $nCompounds no active $nActive hitrate $hitrate")
      csvWriter.writeNext(Array(toxAssay, nCompounds.toString, nActive.toString, hitrate.toString))
    }
  }
}

object buildDfs extends App {

  import Tox21Compounds._

  buildDf()
  buildTestAndScoreDf()

}