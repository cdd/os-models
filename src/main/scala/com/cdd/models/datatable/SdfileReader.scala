package com.cdd.models.datatable

import java.io.File

import com.cdd.models.utils.{HasLogging, SdfileActivityField, TestSdfileProperties}
import org.RDKit.RWMol
import org.apache.log4j.{LogManager, Logger}

object SdfileReader extends HasLogging {

  def sdFileToRdkitDataTable(file: String, properties: Vector[SdfileActivityField], nameField:Option[String]=null): DataTable = {
    logger.info(s"Processing file $file")
    val path = new File(file).getCanonicalPath()
    // build a table of RDKit data
    val rdkitInputBuilder = new RdkitInputBuilder(path, properties, false, true, nameField)
    rdkitInputBuilder.build()
  }

  def sdFileToCdkDataTable(file: String, properties: Vector[SdfileActivityField], nameField:Option[String]=null): DataTable = {
    logger.info(s"Processing file $file")
    // build a table of CDK data
    val path = new File(file).getCanonicalPath()
    val cdkInputBuilder = new CdkInputBuilder(path, properties, false, true, nameField)
    cdkInputBuilder.build()
  }

  def sdFileToDataTable(file: String, properties: Vector[SdfileActivityField],useSdActivityLabels:Boolean=false,
                        nameField:Option[String]=None): DataTable = {

    logger.info(s"Processing file $file")
    // build a table of CDK data
    val path = new File(file).getCanonicalPath()
    val cdkInputBuilder = new CdkInputBuilder(path, properties, false, true, nameField)
    val cdkDt = cdkInputBuilder.build(useSdActivityLabels)

    // build a table of RDKit data
    val rdkitInputBuilder = new RdkitInputBuilder(path, properties, false, false, nameField)
    val rdkitDt = rdkitInputBuilder.build(useSdActivityLabels)

    // TODO if nameField is present check values are the same and remove duplicate column
    // Join them
    InputBuilder.mergeCdkAndRdkit(cdkDt, rdkitDt)
  }

  def sdFileToCsv(sdFile: String, saveFile: String, properties: Vector[SdfileActivityField], nameField:Option[String]=null): Unit = {
    val dt = sdFileToDataTable(sdFile, properties, nameField = nameField)
    dt.exportToFile(saveFile)
  }


}
