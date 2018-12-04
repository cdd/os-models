package com.cdd.models.utils


import java.io.{File, InputStream}

import com.cdd.models.datatable.SdfileReader
import com.google.gson.Gson
import org.apache.log4j.Logger

import scala.io.Source

/**
  * App for processing test directories
  */
object TestDirectoryLocalProperties extends HasLogging {

  /**
    * Read dataset properties from JSON
    *
    * @param in
    * @return
    */
  def readProperties(in: InputStream): Array[TestSdfileProperties] = {
    val jsonString = Source.fromInputStream(in).getLines().mkString(" ")
    // see http://blog.mpieciukiewicz.pl/2012/04/scala-gson-and-option.html for solutions for dealing with Option and gson
    val gson = new Gson()
    gson.fromJson(jsonString, classOf[Array[TestSdfileProperties]])
  }

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
        case "-d" :: value :: tail => optionsMap(map ++ Map('directory -> value), tail)
        case "-f" :: value :: tail => optionsMap(map ++ Map('file -> value), tail)
        case option :: tail => println(s"Unknown option ${option}")
          sys.exit(1)
      }
    }

    val options = optionsMap(Map(), args.toList)

    val directory = options('directory).asInstanceOf[String]

    val testDirectory = new TestDirectoryLocalProperties(new File(directory))
    if (options.contains('file)) {
      val file = options('file).asInstanceOf[String]
        testDirectory.processFile(file)
    }
    else {
      testDirectory.fileNames.foreach { file =>
          testDirectory.processFile(file)
      }
    }
  }
}

trait TestDirectoryProperties {
  def processFile(file: String): String
}

import com.cdd.models.utils.TestDirectoryLocalProperties._

/**
  * Class to create a parquet file for ML from an SD file, by generating a dataframe with descriptor and fingerprint
  * columns calculated from RDKIt and CDK
  *
  * @param directory
  */
class TestDirectoryLocalProperties(directory: File) extends  TestDirectoryProperties with HasLogging {

  val properties = readProperties(Util.openFileAsStreamByName(new File(directory, "recipe.json")))
  val fileNames = properties.map {
    _.file
  }.distinct.toList


  def processFile(file: String): String = {

    logger.info(s"Processing file $file")
    if (!fileNames.contains(file)) {
      throw new IllegalArgumentException(s"no properties information for $file")
    }

    val fileProperties = properties.filter {
      _.file == file
    }.toList

    val path = new File(directory, file).getCanonicalPath()
    val dt = SdfileReader.sdFileToDataTable(path, fileProperties.map(_.toActivityField()).toVector, true)
    val outfile = new File(directory, file.substring(0, file.indexOf('.')) + ".csv.gz").getCanonicalPath
    logger.info(s"saving to $outfile")
    dt.exportToFile(outfile)
    outfile
  }
}
