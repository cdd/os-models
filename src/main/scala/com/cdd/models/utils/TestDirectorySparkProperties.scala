package com.cdd.models.utils

import java.io.{File, InputStream}

import com.cdd.models.datatable.DataTable
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.spark.pipeline._
import com.google.gson.Gson
import org.RDKit.RWMol
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SaveMode}

import scala.io.Source


/**
  * App for processing test directories
  */
object TestDirectorySparkProperties extends HasLogging {

  /**
    * Process a directory of test systems
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: [-s] [-f file] -d directory")
      return
    }

    type OptionMap = Map[Symbol, Any]

    def optionsMap(map: OptionMap, args: List[String]): OptionMap = {
      args match {
        case Nil => map
        case "-o" :: tail => optionsMap(map ++ Map('old -> true), tail)
        case "-d" :: value :: tail => optionsMap(map ++ Map('directory -> value), tail)
        case "-f" :: value :: tail => optionsMap(map ++ Map('file -> value), tail)
        case option :: tail => println(s"Unknown option ${option}")
          sys.exit(1)
      }
    }

    val options = optionsMap(Map(), args.toList)

    val directory = options('directory).asInstanceOf[String]
    val scalable = !options.contains('old)

    val testDirectory = new TestDirectorySparkProperties(new File(directory))
    if (options.contains('file)) {
      val file = options('file).asInstanceOf[String]
      if (scalable)
        testDirectory.processFile(file)
      else
        testDirectory.processFileOld(file)
    }
    else {
      testDirectory.fileNames.foreach { file =>
        if (scalable)
          testDirectory.processFile(file)
        else
          testDirectory.processFileOld(file)
      }
    }
  }
}

import com.cdd.models.utils.TestDirectorySparkProperties._
import com.cdd.models.utils.TestDirectoryLocalProperties.readProperties


/**
  * Class to create a parquet file for ML from an SD file, by generating a dataframe with descriptor and fingerprint
  * columns calculated from RDKIt and CDK
  *
  * @param directory
  */
class TestDirectorySparkProperties(directory: File) extends TestDirectoryProperties with HasLogging {

  val properties = readProperties(Util.openFileAsStreamByName(new File(directory, "recipe.json")))
  val fileNames = properties.map {
    _.file
  }.distinct.toList

  /**
    * The original method for creating a parquet file.  This is not scalable, see Issues.md
    *
    * @param file
    * @return
    */
  def processFileOld(file: String): String = {

    throw new RuntimeException("are you sure?")

    logger.info(s"Processing file $file")
    if (!fileNames.contains(file)) {
      throw new IllegalArgumentException(s"no properties information for $file")
    }

    val fileProperties = properties.filter {
      _.file == file
    }
    val firstProperty = fileProperties(0)
    val path = new File(directory, file).getCanonicalPath()
    val cdkBuilder = new CdkInputBuilder(path)
    cdkBuilder.setActivityField(firstProperty.response)
    if (firstProperty.continuous()) {
      cdkBuilder.setActivityTransform(firstProperty.activityTransform())
    }
    logger.info("Adding CDK information")
    cdkBuilder.build(Configuration.sparkSession, !firstProperty.continuous())
      .addFingerprintColumn(CdkFingerprintClass.ECFP6, FingerprintTransform.Sparse)
      .addFingerprintColumn(CdkFingerprintClass.FCFP6, FingerprintTransform.Sparse)
      .addDescriptorsColumn()
      .addSmilesColumn()
    fileProperties.zipWithIndex.foreach { case (p, i) =>
      if (i > 0) {
        if (p.continuous()) {
          cdkBuilder.setActivityTransform(p.activityTransform())
        }
        cdkBuilder.addActivityColumn(p.response, !p.continuous())
      }
    }

    logger.info("Adding RDKit information")
    val rdkitBuilder = new RdkitInputBuilder(path).append(cdkBuilder)
      .addFingerprintColumn(RdkitFingerprintClass.ECFP6, FingerprintTransform.Sparse)
      .addFingerprintColumn(RdkitFingerprintClass.FCFP6, FingerprintTransform.Sparse)
      .addDescriptorsColumn()
      .addSmilesColumn()

    var df = rdkitBuilder.getDf()
    //.coalesce(1).persist()
    val outfile = new File(directory, file.substring(0, file.indexOf('.')) + "_old.parquet").getCanonicalPath
    logger.info(s"saving to $outfile: no partitions ${df.rdd.partitions.length}")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(outfile)
    //df.unpersist()
    outfile
  }


  /**
    * Current method for creating ML dataset.  Fingerpints are fully sparse, but have been folded in half as scala Sparse
    * vectors are Int indexed
    *
    * @param file
    * @return
    */
  def processFile(file: String): String = {

    logger.info(s"Processing file $file")
    if (!fileNames.contains(file)) {
      throw new IllegalArgumentException(s"no properties information for $file")
    }

    val fileProperties = properties.filter {
      _.file == file
    }

    // build a table of CDK data
    val path = new File(directory, file).getCanonicalPath()
    val cdkInputBuilder = new CdkScalableInputBuilder(path, fileProperties.toList, false, true)
    val cdkDf = cdkInputBuilder.build

    // build a table of RDKit data
    val rdkitInputBuilder = new RdkitScalableInputBuilder(path, fileProperties.toList, false, false)
    val rdkitDf = rdkitInputBuilder.build

    // Join them
    val df = cdkDf.alias("cdk").join(rdkitDf.alias("rdkit"), "no")

    val no = df.count()
    assert(df.count() == cdkDf.count())
    assert(df.count() == rdkitDf.count())
    assert(df.count() == cdkInputBuilder.noMolecules())

    // check that the CDK smiles and RDKit smiles match - we use RDKIt for this as CDK is rubbish for this purpose
    val noMismatches = df.select(col("cdk_smiles").cast(StringType), col("rdkit_smiles").cast(StringType)).rdd.aggregate(0)(
      (acc, value) => (acc + (value match {
        case Row(smiles1: String, smiles2: String) =>
          val m1 = RWMol.MolFromSmiles(smiles1)
          val m2 = RWMol.MolFromSmiles(smiles2)
          val matched = m1.hasSubstructMatch(m2)
          if (!matched) {
            val msg = s"CDK smiles ${smiles1} and RDKit smiles ${smiles2} do not match!"
            logger.error(msg)
          }
          if (matched) 0 else 1
        case _ => 0
      })),
      (acc1, acc2) => (acc1 + acc2)
    )

    // a small proportion of mis-matches is acceptable (assume they are artifacts)
    val proportionMismatched = noMismatches.toDouble/no
    logger.warn(s"N compounds ${no} no mismatches ${noMismatches} Proportion of mismatches ${proportionMismatched}")
    if (proportionMismatched > 0.05) {
      val msg = s"excessive proportion [${proportionMismatched}] of compounds fail to match"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    val outfile = new File(directory, file.substring(0, file.indexOf('.')) + ".parquet").getCanonicalPath
    logger.info(s"saving to $outfile: no partitions ${df.rdd.partitions.length}")
    df.write.format("parquet").mode(SaveMode.Overwrite).save(outfile)
    outfile
  }

}
