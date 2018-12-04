package com.cdd.webservices

import com.cdd.models.tox.{Tox21Tester, Tox21TesterModel}
import com.cdd.models.utils.HasLogging
import com.google.gson.Gson
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.util.log.Slf4jLog
import org.scalatra.{Ok, ScalatraServlet}
import org.scalatra.scalate.ScalateSupport

class Tox21ActivityRequest(val model: String = "ConsensusIncludingScoring", val smiles: Array[String], val targets: Array[String] = Array.empty)

class ModelServlet extends ScalatraServlet with ScalateSupport with HasLogging {
  private val gson = new Gson()

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Hello from the OS Models server.
      </body>
    </html>
  }


  post("/tox21Activity") {
    try {
      val jsonString = request.body
      val tox21ActivityRequest = gson.fromJson(jsonString, classOf[Tox21ActivityRequest])

      val modelName = if (tox21ActivityRequest.model != null) tox21ActivityRequest.model else "ConsensusIncludingScoring"
      val tox21TesterModel = Tox21TesterModel.withName(tox21ActivityRequest.model)
      // gson can set request.targets to null even though default is empty array
      val targets = if (tox21ActivityRequest.targets == null) Array.empty[String] else tox21ActivityRequest.targets
      val results = Tox21Tester.testSmilesAgainstMultiple(tox21TesterModel, tox21ActivityRequest.smiles,
        targets)
      Ok(gson.toJson(results))
    } catch {
      case ex: Throwable =>
        logger.error("tox21Activity post request failed", ex)
        throw ex
    }
  }
}


object JettyLauncher extends App with HasLogging {

  val port = if (args.length == 1) args(0).toInt else 8200

  val server = new Server(port)
  val context = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS)

  context.addServlet(classOf[ModelServlet], "/*")
  context.setLogger(new Slf4jLog())
  context.setResourceBase("src/main/webapp")

  server.start
  server.join
}