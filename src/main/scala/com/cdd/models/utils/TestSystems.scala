package com.cdd.models.utils

import java.io.File
import java.nio.file.Files
import java.util.concurrent.Executors

import com.cdd.models.datatable.DataTable
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object TestSystems {


  /**
    * Enumerated type for project data sets
    */
  object TestSystemDir extends Enumeration {
    type FingerprintClass = Value
    val OSMD_TEST_DATA, CONTINUOUS, DISCRETE = Value
  }

  /**
    * Returns data directory for project data set
    *
    * @param testSystem
    * @return
    */
  def testSuiteToDir(testSystem: TestSystemDir.Value): String = {
    testSystem match {
      case TestSystemDir.OSMD_TEST_DATA => "OSMD test data"
      case TestSystemDir.CONTINUOUS => "continuous"
      case TestSystemDir.DISCRETE => "discrete"
      case _ => throw new IllegalArgumentException(s"Unknown test system $testSystem")
    }
  }


}

object LocalTestSystems {

  import com.cdd.models.utils.TestSystems._

  /**
    * Determines available CSV files on the local filesystem
    *
    * @param dir directory containing parquet files
    * @return
    */
  def localCsvFiles(dir: String): List[String] = {
    var dirPath = if (dir.startsWith("/")) dir else "data" + File.separator + dir
    val path = Util.getProjectFilePath(dirPath).toPath
    Files.newDirectoryStream(path, "*.csv.gz").map {
      _.toString
    }.toList.sortBy(Util.fileSize(_))
  }

  /**
    * Create a TestSystem class for a particular test directory
    *
    * @param testSystem project data set
    * @param func       function to apply to each Parquet file
    * @return
    */
  def testSystemsFactory(testSystem: TestSystemDir.Value, func: (String, DataTable) => Unit): LocalTestSystems = {
    val testSystemDir = testSuiteToDir(testSystem)
    val files = localCsvFiles(testSystemDir)
    new LocalTestSystems(files, func)
  }

  def prepareResultFile(fileName: String, removeExistingResults: Boolean = false): (String, Set[String]) = {
    val resultsFile = Util.getProjectFilePath(s"data/pipeline_results/${fileName}")
    var currentResults = Set[String]()
    Util.runWithLockFile(resultsFile.getAbsolutePath + ".lck", () => {
      if (resultsFile.exists()) {
        if (removeExistingResults) {
          resultsFile.delete()
        }
        else {
          val currentNames = DataTable.loadFile(resultsFile.getAbsolutePath).column("NAME").uniqueValues().asInstanceOf[Vector[String]]
          currentResults = currentNames.toSet
        }
      }
    })
    (resultsFile.getAbsolutePath, currentResults)
  }
}

object SparkTestSystems {

  import com.cdd.models.utils.TestSystems._

  /**
    * Determines available parquet files on the local filesystem
    *
    * @param dir directory containing parquet files
    * @return
    */
  def localParquetFiles(dir: String): List[String] = {
    var dirPath = if (dir.startsWith("/")) dir else "data" + File.pathSeparator + dir
    val path = Util.getProjectFilePath(dirPath).toPath
    Files.newDirectoryStream(path, "*.parquet").map {
      _.toString
    }.toList
  }

  /**
    * Determines available parquet files on hadoop filesystem
    *
    * @param dir directory containing parquet files
    * @return
    */
  def hadoopParquetFiles(dir: String): List[String] = {
    val hdfs = Util.hdfs()
    // also try hdfs.listFiles(new Path(Util.getHadoopPath()+"/"+dir), false)
    val path = new Path(Util.hadoopPath() + "/data/" + dir + "/*.parquet")
    hdfs.globStatus(path).map {
      _.getPath.toString
    }.sortBy(Util.hadoopFolderSize(_, Some(hdfs))).toList
  }


  /**
    * Create a TestSystem class for a particular test directory
    *
    * @param testSystem project data set
    * @param hadoop     set true for hadoop path
    * @param func       function to apply to each Parquet file
    * @return
    */
  def testSystemsFactory(testSystem: TestSystemDir.Value, hadoop: Boolean, func: (String, DataFrame) => Unit): SparkTestSystems = {
    val testSystemDir = testSuiteToDir(testSystem)
    val files = if (hadoop) hadoopParquetFiles(testSystemDir) else localParquetFiles(testSystemDir)
    new SparkTestSystems(files, func)
  }

  /**
    * Finds the hadoop path of a parquet results file.  Deletes it if requested.  Alternatively, if the file exsists
    * results a set of the test systems already evaluated.
    *
    * @param fileName
    * @param removeExistingResults
    * @return tuple of full hadoop path and option of any existing results
    */
  def prepareResultFile(fileName: String, removeExistingResults: Boolean = false): (String, Option[Set[String]]) = {
    val resultsFile = Util.hadoopPath() + s"/data/pipeline_results/${fileName}"
    val hdfs = Util.hdfs()
    val hadoopPath = new Path(resultsFile)
    var currentResults: Option[Set[String]] = None
    if (hdfs.exists(hadoopPath)) {
      if (removeExistingResults) {
        hdfs.delete(hadoopPath, true)
      }
      else {
        val results = Configuration.sparkSession.read.option("headers", true).parquet(resultsFile)
        import results.sparkSession.implicits._
        currentResults = Some(results.select("name").map { r => r.getString(0) }.collect().toSet)
      }
    }
    (resultsFile, currentResults)
  }
}


trait TestSystems extends HasLogging{

  val testSystems: List[String]


  protected def filterSystems(nSystems: Int, includeNames: List[String], excludeNames: List[String]): List[String] = {
    testSystems.take(nSystems)
      .filter { p =>
        if (includeNames.isEmpty) true else includeNames.contains(name(p))
      }
      .filter { p =>
        if (excludeNames.isEmpty) true else !excludeNames.contains(name(p))
      }
  }

  protected def name(path: String): String = {
    var rtn = path match {
      case p if p.endsWith(".parquet") => p.dropRight(8)
      case p if p.endsWith(".csv.gz") => p.dropRight(7)
      case p => p
    }
    val index = rtn.lastIndexOf('/')
    if (index != -1)
      rtn = rtn.drop(index + 1)
    rtn
  }

  /**
    * Apply function to all parquet files simultaneously
    */
  protected def applyFunctionToTestSystemsConcurrently(nSystems: Int, includeNames: List[String], excludeNames: List[String], func: (String) => Unit): Unit = {

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(20))
    val futures = filterSystems(nSystems, includeNames, excludeNames)
      .zipWithIndex.map { case (p, i) =>

      logger.warn(s"Submitting future for job ${i} directory ${p.toString}")
      val future: Future[Boolean] = Future {
        func.apply(p)
        true
      }

      future.onComplete {
        case Success(_) => logger.warn(s"Finished applying function on file ${p}")
        case Failure(ex) => logger.error(s"Error on applying function on file ${p}", ex)
      }

      future
    }

    logger.warn("All jobs submitted")
    val allFutures = Future.sequence(futures)
    Await.result(allFutures, Duration.Inf)
    logger.warn("All jobs done")
    ec.shutdownNow()
  }


  /**
    * Apply function to all parquet files sequentially
    */
  protected def applyFunctionToTestSystemsSequentially(nSystems: Int, includeNames: List[String], excludeNames: List[String], func: (String) => Unit): Unit = {
    filterSystems(nSystems, includeNames, excludeNames)
      .zipWithIndex
      .foreach { case (p, i) =>
        logger.warn(s"Running function ${i} directory ${p.toString}")
        func.apply(p)
      }
  }

}

/**
  * A class for managing running a function over a set of test systems
  *
  * @param testSystems A list of parquet files
  * @param func        A function to apply to dataframes created from those files
  */
class SparkTestSystems(val testSystems: List[String], func: (String, DataFrame) => Unit) extends TestSystems {

  import com.cdd.models.utils.TestSystems._

  /**
    * Apply function to all parquet files simultaneously
    */
  def applyFunctionToTestSystemsConcurrently(nSystems: Int = 100, includeNames: List[String] = List(), excludeNames: List[String] = List()): Unit = {

    val spark = Configuration.sparkSession

    applyFunctionToTestSystemsConcurrently(nSystems, includeNames, excludeNames, (p) => {
      logger.warn(s"Reading parquet directory ${p}")
      val df = spark.read.parquet(p)
      logger.warn(s"Parquet ${p} read")
      func(name(p), df)
    })
  }

  /**
    * Apply function to all parquet files sequentially
    */
  def applyFunctionToTestSystemsSequentially(nSystems: Int = 100, includeNames: List[String] = List(), excludeNames: List[String] = List()): Unit = {
    val spark = Configuration.sparkSession

    applyFunctionToTestSystemsSequentially(nSystems, includeNames, excludeNames, (p) => {
      logger.warn(s"Reading parquet directory ${p}")
      val df = spark.read.parquet(p)
      logger.warn(s"Parquet ${p} read")
      func(name(p), df)
    })
  }

}

class LocalTestSystems(val testSystems: List[String], func: (String, DataTable) => Unit) extends TestSystems {

  import com.cdd.models.utils.TestSystems._

  def applyFunctionToTestSystemsConcurrently(nSystems: Int = 100, includeNames: List[String] = List(), excludeNames: List[String] = List()): Unit = {

    val spark = Configuration.sparkSession

    applyFunctionToTestSystemsConcurrently(nSystems, includeNames, excludeNames, (p) => {
      logger.warn(s"Reading CSV file  ${p}")
      val dt = DataTable.loadFile(p)
      logger.warn(s"CSV ${p} read")
      func(name(p), dt)
    })
  }

  def applyFunctionToTestSystemsSequentially(nSystems: Int = 100, includeNames: List[String] = List(), excludeNames: List[String] = List()): Unit = {

    applyFunctionToTestSystemsSequentially(nSystems, includeNames, excludeNames, (p) => {
      logger.warn(s"Reading CSV file ${p}")
      val dt = DataTable.loadFile(p)
      logger.warn(s"Read datatable from ${p} read")
      func(name(p), dt)
    })
  }

}