package com.cdd.models.vault

import java.io.IOException
import java.net.SocketTimeoutException

import com.cdd.models.utils.{HasLogging, Modifier, Util}
import com.cdd.models.vault.RequestMethod.RequestMethod
import com.google.gson.Gson
import com.google.gson.internal.LinkedTreeMap
import org.apache.commons.httpclient.{HttpException, HttpStatus}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost, HttpRequestBase}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.conn.HttpHostConnectException
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

object RequestMethod extends Enumeration {
  type RequestMethod = Value
  val GET, POST, DELETE, HEAD, OPTIONS = Value
}

object VaultDownload extends HasLogging {

  private val apiToken = System.getenv("CDD_VAULT_API_TOKEN")
  if (apiToken == null) {
    throw new IllegalArgumentException("Please set environment variable CDD_VAULT_API_TOKEN to your vault API token")
  }
  private val baseUrl = "https://app.collaborativedrug.com/api/v1"
  private[vault] val gson = new Gson()

  def downloadDatasets(vaultId: Int): Array[DataSet] = {
    val url = s"$baseUrl/vaults/$vaultId/data_sets"
    val jsonString = downloadFromVaultAsString(url)
    gson.fromJson(jsonString, classOf[Array[DataSet]])
  }

  def downloadProjects(vaultId: Int): Array[Project] = {
    val url = s"$baseUrl/vaults/$vaultId/projects"
    val jsonString = downloadFromVaultAsString(url)
    gson.fromJson(jsonString, classOf[Array[Project]])
  }

  def downloadProtocol(vaultId: Int, protocolId: Int): Protocol = {
    val url = s"$baseUrl/vaults/$vaultId/protocols/$protocolId"
    val jsonString = downloadFromVaultAsString(url)
    gson.fromJson(jsonString, classOf[Protocol])
  }

  def downloadProtocolsForDataset(vaultId: Int, datasetId: Int): Vector[Protocol] = {
    val url = s"$baseUrl/vaults/$vaultId/protocols"
    val protocols = downloadPagedObjectList[Protocol](url, vaultId, params = Map("data_sets" -> datasetId.toString))
    protocols.filter(_.dataSet.id == datasetId)
  }

  def downloadProtocolsForProject(vaultId: Int, projectId: Int): Vector[Protocol] = {
    val url = s"$baseUrl/vaults/$vaultId/protocols"
    val protocols = downloadPagedObjectList[Protocol](url, vaultId, params = Map("projects" -> projectId.toString))
    protocols.filter(_.projects.map(_.id).contains(projectId))
  }

  private def protocolDataReadoutUrl(vaultId: Int, protocol: Protocol, runIds: Option[Vector[Int]] = None, datasetId: Option[Int] = None) = {
    val url = s"$baseUrl/vaults/$vaultId/protocols/${protocol.id}/data"
    var params = Map[String, String]()
    runIds match {
      case Some(ids) => params += ("runs" -> ids.mkString(","))
      case None =>
    }
    datasetId match {
      case Some(id) => params += ("data_sets" -> id.toString)
      case None =>
    }
    (url, params)
  }

  def determineProtocolReadoutCount(vaultId: Int, protocol: Protocol, runIds: Option[Vector[Int]] = None, datasetId: Option[Int] = None): Int = {
    val (url, params) = protocolDataReadoutUrl(vaultId, protocol, runIds, datasetId)
    val jsonString = downloadFromVaultAsString(url, params = params)
    val objectList = gson.fromJson(jsonString, classOf[ObjectListContainer])
    objectList.count
  }

  def downloadProtocolData(vaultId: Int, protocol: Protocol, runIds: Option[Vector[Int]] = None, datasetId: Option[Int] = None): Vector[ProtocolDataReadouts] = {
    val (url, params) = protocolDataReadoutUrl(vaultId, protocol, runIds, datasetId)
    // val jsonString = downloadFromVaultAsString(url, params = params)
    val protocolDataList = downloadPagedObjectList[ProtocolData](url, vaultId, params)

    protocolDataList.map { data =>
      val readoutValues = data.readoutsTree.asInstanceOf[LinkedTreeMap[String, Any]].entrySet().asScala.flatMap { entry =>
        val readoutId = entry.getKey.toInt
        val readoutDefinitionOption = protocol.readoutDefinitions.find(_.id == readoutId)
        assert(readoutDefinitionOption.isDefined)
        val readoutDefinition = readoutDefinitionOption.get
        val dataClazz = readoutDefinition.readoutType()
        var modifier = Modifier.EQUAL
        val value = entry.getValue match {
          case m: LinkedTreeMap[String, Object] =>
            m.entrySet().asScala.foreach { e =>
              val k = e.getKey
              val v = e.getValue

              k match {
                case "value" =>
                case "modifier" => modifier = Modifier.fromString(v.toString)
                case "note" =>
                case "outlier_type" =>
                case _ => throw new IllegalArgumentException(s"Unknown readout key $k [value $v] class ${v.getClass}")
              }
            }
            m.get("value") match {
              case null => None
              case v: Any => Some(readValue(v, dataClazz))
            }
          case v => Some(readValue(v, dataClazz))
        }
        value match {
          case Some(v) =>
            val rv = new ReadoutValue[Any](dataClazz.asInstanceOf[Class[Any]], protocol.id, readoutDefinition.id, v, modifier)
            Some(rv)
          case None => None
        }
      }.toVector
      new ProtocolDataReadouts(data, readoutValues)
    }
  }

  private def readValue[T](o: Any, clazz: Class[T]): T = {
    o match {
      case str: String =>
        val v = clazz match {
          case c if c == classOf[Double] => str.toDouble
          case c if c == classOf[String] => str
          case c if c == classOf[Int] => str.toDouble
          case _ => throw new IllegalArgumentException(s"Unable to read value of class $clazz")
        }
        v.asInstanceOf[T]
      case d: Double =>
        require(clazz == classOf[Double])
        d.asInstanceOf[T]
    }
  }

  def downloadMolecules(vaultId: Int, moleculeIds: Vector[Int], datasetId: Option[Int] = None): Vector[Molecule] = {
    val url = s"$baseUrl/vaults/$vaultId/molecules"
    val molecules = moleculeIds.grouped(500).flatMap { ids =>
      var params = Map("molecules" -> ids.mkString(","))
      if (datasetId.isDefined) {
        params += "data_sets" -> datasetId.get.toString
      }
      //downloadPagedObjectList[Molecule](url, vaultId, params = params)
      val data = downloadFromVaultAsyncAsString(url, vaultId, params)
      data.convertObjects[Molecule]
    }.toVector
    return molecules
  }

  private def downloadPagedObjectList[T: ClassTag](url: String, vaultId: Int, params: Map[String, String] = Map(),
                                                   action: RequestMethod = RequestMethod.GET,
                                                   connectTimeout: Int = 5000, readTimeout: Int = 60000): Vector[T] = {
    var offset = 0
    val page_size = 500
    var paramMap = params + ("page_size" -> page_size.toString)
    var notFinished = true
    var count: Option[Int] = None
    val results = ArrayBuffer[Object]()
    while (notFinished) {
      val jsonString = downloadFromVaultAsString(url, paramMap + ("offset" -> offset.toString),
        action, connectTimeout, readTimeout)
      var objectList = gson.fromJson(jsonString, classOf[ObjectListContainer])
      count match {
        case Some(c) => require(objectList.count == c)
        case None =>
          count = Some(objectList.count)
          // for large requests use async export
          if (count.get > 2000 && action == RequestMethod.GET) {
            logger.info(s"Large object list of ${count.get} items: fetching using async API")
            objectList =  downloadFromVaultAsyncAsString(url, vaultId, params, connectTimeout, readTimeout)
            assert(objectList.objects.length == count.get)
          }
      }
      assert(count.isDefined)
      val objects = objectList.objects
      if (objects.isEmpty)
        notFinished = false
      else {
        results ++= objects
        if (objects.length == count.get || objectList.count < objectList.offset + objectList.pageSize)
          notFinished = false
        offset += page_size
      }
    }

    require(results.size == count.get)
    val ctag = implicitly[ClassTag[T]]
    results.map { r =>
      val str = gson.toJson(r)
      gson.fromJson(str, ctag.runtimeClass.asInstanceOf[Class[T]])
    }.toVector
  }

  private def downloadFromVaultAsString(url: String, params: Map[String, String] = Map(),
                                        action: RequestMethod = RequestMethod.GET,
                                        connectTimeout: Int = 5000, readTimeout: Int = 60000): String = {
    val request = action match {
      case RequestMethod.GET => new HttpGet(url)
      case RequestMethod.POST => new HttpPost(url)
      case _ => throw new IllegalArgumentException
    }

    request.setHeader("X-CDD-Token", apiToken)

    val config = RequestConfig.custom()
      .setConnectTimeout(connectTimeout)
      .setSocketTimeout(readTimeout)
      .build()

    if (params.nonEmpty) {
      action match {
        case RequestMethod.POST =>
          val nameValuePairs = params.map { case (n, v) => new BasicNameValuePair(n, v) }
          request.asInstanceOf[HttpPost].setEntity(new UrlEncodedFormEntity(nameValuePairs.toList.asJava))
        case RequestMethod.GET =>
          val builder = new URIBuilder(request.getURI)
          params.foreach { case (n, v) => builder.addParameter(n, v) }
          val uri = builder.build()
          logger.info(s"Fetching from $uri")
          request.setURI(uri)
        case _ =>
          throw new IllegalArgumentException
      }
    }

    sendRequestAndReturnResponse(config, request)
  }

  private def sendRequestAndReturnResponse(config: RequestConfig, request: HttpRequestBase): String = {
    var nAttempts = 0
    while (true) {
      try {
        return Util.using(HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
          client =>
            val response = client.execute(request)
            val entity = response.getEntity
            var content = ""
            if (entity != null) {
              content = Util.using(entity.getContent) {
                is =>
                  Source.fromInputStream(is).getLines().mkString
              }
            }
            val code = response.getStatusLine.getStatusCode
            if (code != HttpStatus.SC_OK) {
              val msg = s"Http status not OK, status: ${response.getStatusLine} content: ${content}"
              logger.error(msg)
              throw new HttpException(msg)
            }
            content
        }
      }
      catch {
        case ex:IOException=>
          nAttempts += 1
          logger.warn(s"sendRequestAndReturnResponse failed on attempt ${nAttempts}")
          if (nAttempts == 10)
            throw ex
          logger.info(s"waiting 1 min and retrying..")
          Thread.sleep(60000)
      }
    }
    throw new IllegalStateException()
  }

  class AsyncExport(val id: Int, val status: String)

  private def downloadFromVaultAsyncAsString(url: String, vaultId: Int, params: Map[String, String] = Map(),
                                             //action: RequestMethod = RequestMethod.GET,
                                             connectTimeout: Int = 5000, readTimeout: Int = 60000) = {
    val request = new HttpGet(url)

    request.setHeader("X-CDD-Token", apiToken)

    val config = RequestConfig.custom()
      .setConnectTimeout(connectTimeout)
      .setSocketTimeout(readTimeout)

      .build()

    val builder = new URIBuilder(request.getURI)
    builder.addParameter("async", "true")
    params.foreach { case (n, v) => builder.addParameter(n, v) }
    val uri = builder.build()
    logger.info(s"Fetching from $uri")
    request.setURI(uri)

    var asyncContent = sendRequestAndReturnResponse(config, request)
    var asyncExport = gson.fromJson(asyncContent, classOf[AsyncExport])
    logger.info(s"Async export request id ${asyncExport.id} status ${asyncExport.status}")

    while (asyncExport.status != "finished") {
      Thread.sleep(5000L)

      val url = s"$baseUrl/vaults/$vaultId/export_progress/${asyncExport.id}"
      asyncContent = downloadFromVaultAsString(url)
      asyncExport = gson.fromJson(asyncContent, classOf[AsyncExport])
      logger.info(s"Async export request id ${asyncExport.id} status ${asyncExport.status}")
    }

    val downloadUrl = s"$baseUrl/vaults/$vaultId/exports/${asyncExport.id}"
    logger.info("Downloading async export")
    val asyncJsonString =  downloadFromVaultAsString(downloadUrl)
    gson.fromJson(asyncJsonString, classOf[ObjectListContainer])
  }
}
