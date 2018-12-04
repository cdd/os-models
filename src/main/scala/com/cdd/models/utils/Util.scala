
package com.cdd.models.utils


import java.io._
import java.net.{HttpURLConnection, SocketTimeoutException, URI, URL}
import java.nio.channels.FileLock
import java.util
import java.util.zip.GZIPInputStream

import com.google.gson.Gson
import org.apache.commons.httpclient.HttpStatus
import org.apache.commons.io.IOUtils
import org.apache.commons.math.special.Erf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.{HttpException, NameValuePair}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.ml.linalg.SparseVector
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.internal.chartpart.Chart
import org.knowm.xchart.{BitmapEncoder, XYChart, XYChartBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.reflect.{ClassTag, _}


/**
  * A trait to attach a logger to a class
  */
trait HasLogging {
  @transient
  protected lazy val logger: Logger = LogManager.getLogger(getClass)
}

trait LoadsRdkit {
  try {
    System.loadLibrary("GraphMolWrap")
  } catch {
    case _: UnsatisfiedLinkError =>
      Util.loadLibrary("GraphMolWrap")
  }
}

/**
  * Created by gareth on 5/15/17.
  */
object Util extends HasLogging {
  private val gson = new Gson()

  def openResourceAsStream(resource: String): InputStream = {
    val inStream = Option(getClass.getResourceAsStream(resource))
    if (inStream == None) {
      throw new IllegalArgumentException(s"Unable to open resource $resource")
    }
    inStream.get
  }

  def openGzippedResourceAsStream(resource: String): InputStream = {
    new GZIPInputStream(openResourceAsStream(resource))
  }

  def openResourceAsStreamByName(resource: String): InputStream = {
    if (resource.toLowerCase.endsWith(".gz")) {
      openGzippedResourceAsStream(resource)
    }
    else {
      openResourceAsStream(resource)
    }
  }

  def openGzippedFileAsStream(file: File): InputStream = {
    new GZIPInputStream(new FileInputStream(file))
  }

  def openFileAsStreamByName(file: File): InputStream = {
    if (file.getPath.toLowerCase.endsWith(".gz")) {
      openGzippedFileAsStream(file)
    }
    else {
      new FileInputStream(file)
    }
  }

  def openPathAsStreamByName(filename: String, resource: Boolean = false): InputStream = {
    if (resource) {
      openResourceAsStreamByName(filename)
    } else {
      openFileAsStreamByName(getProjectFilePath(filename))
    }
  }

  private def stringToDouble(s: String): Option[Double] = {
    val number = s.filter {
      !"<>=".contains(_)
    }
    try {
      Some(number.toDouble)
    }
    catch {
      case _: NumberFormatException => None
    }

  }

  def activityContinuousValue(field: String): Option[Double] = {
    val numbers = field.split("\\s").map {
      stringToDouble
    }.filter {
      _.isDefined
    }
    if (numbers.length > 0) numbers(0) else None
  }

  private val intRegExp = "(\\d+)".r

  def activityCategoryValue(field: String): Option[Double] = {
    field.trim.toLowerCase() match {
      case intRegExp(num) => Some(num.toDouble)
      case "false" => Some(0.0)
      case "true" => Some(1.0)
      case _ => {
        logger.warn(s"Unable to assign activity from value ${field}")
        None
      }
    }
  }

  def runWithLockFile(lockFileName: String, block: () => Unit): Unit = synchronized {

    val lockFile = new File(lockFileName)
    val channel = new RandomAccessFile(lockFile, "rw").getChannel

    // Issues with lock and local variables, so don't covert to option or use in case
    var lock: FileLock = null
    try {
      lock = channel.lock()
      block.apply()
    }
    finally
      if (lock == null)
        throw new RuntimeException("Failed to acquire lock!")
      else lock.release()
  }

  def withSerializedFile[A <: Serializable : ClassTag](cacheFile: File)(block: () => A): A = {
    if (cacheFile.exists()) {
      deserializeObject[A](cacheFile.getAbsolutePath)
    } else {
      val data = block()
      serializeObject[A](cacheFile.getAbsolutePath, data)
      data
    }
  }

  def using[A <: {def close()}, B]
  (resource: A)
  (block: A => B): B = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  def getClassPath(): String = {
    val path = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
    return path.getParent
  }

  def getProjectPath(): File = {
    new File(getClassPath()).getParentFile.getCanonicalFile
  }

  def getProjectFilePath(filename: String): File = {
    if (filename.startsWith("/")) {
      new File(filename).getCanonicalFile
    } else {
      new File(getProjectPath(), filename).getCanonicalFile
    }
  }

  def toJavaDouble(v: Option[Double]): java.lang.Double = {
    v match {
      case Some(d) => d
      case _ => null
    }
  }

  // private SparseVector method copied from org/apache/spark/ml/linalg/Vectors.scala
  def slice(svIn: SparseVector, selectedIndices: Array[Int]): SparseVector = {
    var currentIdx = 0
    val (sliceInds, sliceVals) = selectedIndices.flatMap { origIdx =>
      val iIdx = java.util.Arrays.binarySearch(svIn.indices, origIdx)
      val i_v = if (iIdx >= 0) {
        Iterator((currentIdx, svIn.values(iIdx)))
      } else {
        Iterator()
      }
      currentIdx += 1
      i_v
    }.unzip
    new SparseVector(selectedIndices.length, sliceInds.toArray, sliceVals.toArray)
  }

  def hadoopPath(): String = if (Configuration.runningInAws()) "s3://os-models2/" else "hdfs://localhost:9005/os-models"

  def hdfs(): FileSystem = FileSystem.get(new URI(hadoopPath()), Configuration.sparkContext.hadoopConfiguration)

  private[utils] def loadLibrary(library: String): Unit = {
    var libName = library
    val osName = System.getProperty("os.name")
    // this code has only ever been used on Linux
    libName = if (osName == "Linux") {
       s"lib$libName.so"
    }
    else if (osName.toLowerCase.contains("windows")) {
      // don't know what windows name should be
      s"lib$libName.dll"
    }
    else if (osName.toLowerCase.contains("mac")) {
      s"lib$libName.jnilib"
    }
    else {
      throw new IllegalArgumentException(s"Unable to load library from resource on $osName platform")
    }
    val path = "lib/" + osName.replace(' ', '_') + "_" +
      System.getProperty("os.arch").replace(' ', '_') + "/" + libName
    val libFile = getProjectFilePath(path)
    if (!libFile.exists())
      throw new RuntimeException(s"Unable to load library ${libFile.getAbsolutePath}")
    System.load(libFile.getAbsolutePath)
  }


  /**
    * Determine the space used by a hadoop folder or file
    *
    * @param path
    * @return
    */
  def hadoopFolderSize(path: String, fileSystem: Option[FileSystem] = None): Long = {
    val hadoopPath = new Path(path)
    val fs = if (fileSystem == None) hdfs() else fileSystem.get
    fs.getContentSummary(hadoopPath).getSpaceConsumed
  }

  def fileSize(path: String): Long = {
    new File(path).length()
  }

  def gridPlot(file: String, chartsIn: Vector[XYChart], cols: Int = 3): Unit = {

    var charts = chartsIn
    var nCharts = charts.length
    var rows = charts.length / cols
    if (nCharts % cols > 0) {
      val width = chartsIn(0).getWidth
      val height = chartsIn(0).getHeight
      rows += 1
      val nExtra = rows * cols - nCharts
      // need to pad grid with dummy charts or bitmap save fails
      for (_ <- 0 until nExtra) {
        val chart = new XYChartBuilder().xAxisTitle("X").yAxisTitle("Y").width(width).height(height).build()
        charts = charts :+ chart
      }
    }

    BitmapEncoder.saveBitmap(charts.asJava.asInstanceOf[java.util.List[Chart[_, _]]], rows, cols, file, BitmapFormat.PNG)
  }

  /**
    * Opens the file while ensuring that the parent directory exists
    *
    * @param file
    * @return
    */
  def openFilePath(file: File): FileWriter = {
    file.getParentFile.mkdirs()
    new FileWriter(file)
  }

  /**
    * Download and return content from a URL, with timeouts
    *
    * @param url
    * @param connectTimeout
    * @param readTimeout
    * @param requestMethod
    * @throws java.io.IOException
    * @throws java.net.SocketTimeoutException
    * @return
    */
  @throws(classOf[IOException])
  @throws(classOf[SocketTimeoutException])
  def getFromUrl(url: String, connectTimeout: Int = 5000, readTimeout: Int = 5000,
                 requestMethod: String = "GET"): String = {
    val conn = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(connectTimeout)
    conn.setReadTimeout(readTimeout)
    conn.setRequestMethod(requestMethod)
    using(conn.getInputStream) {
      is =>
        Source.fromInputStream(is).mkString
    }
  }


  private def httpClient(connectTimeout: Int, readTimeout: Int): CloseableHttpClient = {
    val config = RequestConfig.custom()
      .setConnectTimeout(connectTimeout)
      .setSocketTimeout(readTimeout)
      .build()
    HttpClientBuilder.create().setDefaultRequestConfig(config).build()
  }

  def postNameValuePairsToUrl(url: String, nameValuePairs: Map[String, String],
                              connectTimeout: Int = 5000, readTimeout: Int = 60000): String = {
    val post = new HttpPost(url)
    val pairs = new util.ArrayList[NameValuePair]()
    nameValuePairs.foreach {
      case (name, value) => pairs.add(new BasicNameValuePair(name, value))
    }
    post.setEntity(new UrlEncodedFormEntity(pairs))
    using(httpClient(connectTimeout, readTimeout)) {
      client =>
        val response = client.execute(post)
        val entity = response.getEntity
        var content = ""
        if (entity != null) {
          content = using(entity.getContent) {
            is =>
              Source.fromInputStream(is).getLines().mkString
          }
        }
        content
    }
  }

  @throws[HttpException]
  def downloadBinaryDataToStream(url: String, nameValuePairs: Map[String, String], out: OutputStream,
                                 connectTimeout: Int = 5000, readTimeout: Int = 60000): Unit = {
    val post = new HttpPost(url)
    val pairs = new util.ArrayList[NameValuePair]()
    nameValuePairs.foreach {
      case (name, value) => pairs.add(new BasicNameValuePair(name, value))
    }
    post.setEntity(new UrlEncodedFormEntity(pairs))
    using(httpClient(connectTimeout, readTimeout)) {
      client =>
        val response = client.execute(post)
        val code = response.getStatusLine.getStatusCode
        if (code != HttpStatus.SC_OK) {
          val msg = s"Http status not OK: ${response.getStatusLine}"
          logger.error(msg)
          throw new HttpException(msg)
        }
        val entity = response.getEntity
        require(entity != null)
        try {
          using(entity.getContent) {
            in =>
              IOUtils.copy(in, out)
          }
        } finally {
          IOUtils.closeQuietly(out)
        }
    }
  }

  @throws[HttpException]
  def postJsonObjectToUrl[A: ClassTag, B: ClassTag](url: String, value: A,
                                                    connectTimeout: Int = 5000,
                                                    readTimeout: Int = 60000): (B, String) = {
    val post = new HttpPost(url)
    val stringValue = gson.toJson(value, classTag[A].runtimeClass)
    post.setEntity(new StringEntity(stringValue))
    post.setHeader("Content-type", "application/json")
    val content = using(httpClient(connectTimeout, readTimeout)) {
      client =>
        val response = client.execute(post)
        val code = response.getStatusLine.getStatusCode
        if (code != HttpStatus.SC_OK) {
          val msg = s"Http status not OK: ${response.getStatusLine}"
          logger.error(msg)
          throw new HttpException(msg)
        }
        val entity = response.getEntity
        var content = ""
        if (entity != null) {
          content = using(entity.getContent) {
            is =>
              Source.fromInputStream(is).getLines().mkString
          }
        }
        content
    }
    (gson.fromJson(content, classTag[B].runtimeClass), content)
  }

  def serializeObject[A <: Serializable](file: String, obj: A): Unit = {
    using(new ObjectOutputStream(new FileOutputStream(file))) { out =>
      out.writeObject(obj)
    }
  }

  def deserializeObject[A <: Serializable : ClassTag](file: String): A = {
    logger.info(s"Deserializing from $file")
    using(new ObjectInputStream(new FileInputStream(file))) { in =>
      in.readObject().asInstanceOf[A]
    }
  }

  class CartesianProductTooLargeException(val msg: String) extends Exception(msg)

  def cartesianProduct[T](list: Vector[Vector[T]]): Vector[Vector[T]] = {
    list.foreach { l =>
      if (l.size > 1000) {
        throw new RuntimeException(s"List of size ${l.size} in Cartesian product is too large!")
      }
    }
    list match {
      case xs +: IndexedSeq() => xs map (Vector(_))
      case x +: xs => for {
        i <- x
        j <- cartesianProduct(xs)
      } yield Vector(i) ++ j
    }
  }

  def commonKeysAndValues[A](maps: Vector[Map[String, A]]): Map[String, A] = {
    val commonKeysList = maps.map {
      _.keySet.toSet
    }
    val commonKeys = commonKeysList.foldRight(commonKeysList(0)) { case (k, c) => c.intersect(k) }
    val start = mutable.Map[String, A]() ++ maps(0).filterKeys(commonKeys.contains)
    maps.tail.foldRight(start) { case (m, intersect) =>
      intersect.keySet.foreach { k =>
        assert(m.contains(k))
        if (intersect(k) != m(k))
          intersect.remove(k)
      }
      intersect
    }.toMap
  }

  def normalCdf(x: Double, sigma: Double = 1.0): Double = {
    0.5 * (1.0 + Erf.erf(x / (sigma * Math.sqrt(2.0))))
  }


  def time[A](block: => A): (A, Long) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    val elapsedTime = t1-t0
    logger.info(s"Elapsed time: $elapsedTime ms")
    (result, elapsedTime)
  }

  // see https://stackoverflow.com/questions/16257378/is-there-a-generic-way-to-memoize-in-scala
  def memoize[I, O](f: I => O): I => O = new mutable.HashMap[I, O]() {
    self =>
    override def apply(key: I) = self.synchronized(getOrElseUpdate(key, f(key)))
  }
}
