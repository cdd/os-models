package com.cdd.webservices

import javax.servlet.ServletContext
import org.scalatra.LifeCycle

// Don't think this is needed for direct App use, but may be required for maven Jetty plugin

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext): Unit = {
    context.mount(new ModelServlet, "/*")
  }
}