package com.cdd.models.utils

import javax.swing.SwingUtilities
import org.knowm.xchart.{XChartPanel, XYChart}
import scalafx.embed.swing.SwingNode

trait HasSwingCharts {


  implicit def funToRunnable(fun: () => Unit) = new Runnable() {
    def run() = fun()
  }


  protected def chartNode(chart: XYChart) = {
    val swingNode = new SwingNode()
    SwingUtilities.invokeLater {
      () =>
        val swingImage = new XChartPanel[XYChart](chart)
        swingNode.setContent(swingImage)
    }
    swingNode
  }
}
