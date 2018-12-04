package com.cdd.models.vault

import com.cdd.models.universalmetric.NormalizedHistogram
import com.cdd.models.utils.HasSwingCharts
import javafx.scene.control.Label
import javax.swing.SwingUtilities
import org.apache.commons.lang3.StringUtils
import org.knowm.xchart.{XChartPanel, XYChart}
import scalafx.animation.{KeyFrame, Timeline}
import scalafx.application
import scalafx.application.{JFXApp, Platform}
import scalafx.event.ActionEvent
import scalafx.scene.{Node, Scene}
import scalafx.scene.control.{Button, CheckBox, ScrollPane, Tooltip}
import scalafx.scene.control.ScrollPane.ScrollBarPolicy
import scalafx.util.Duration
import scalafx.Includes._
import scalafx.embed.swing.SwingNode
import scalafx.geometry.Insets
import scalafx.scene.layout.{GridPane, HBox, Pane, StackPane}

import scala.collection.mutable.ArrayBuffer

object HistogramViewer extends JFXApp {

  stage = new application.JFXApp.PrimaryStage {
    title = "Histogram Viewer"
  }

  build()

  def build() = {
    stage.scene = new HistogramScene()
  }
}

class HistogramScene() extends Scene(800, 600) with HasSwingCharts {

  var histogramIterator = new VaultHistogramIterator
  val cdfCheckBox = new CheckBox(text = "CDF") {
    onAction = (e: ActionEvent) => {
      redraw()
    }
  }
  val normalCheckBox = new CheckBox(text = "Normal") {
    onAction = (e: ActionEvent) => {
      redraw()
    }
  }
  val cutoffCheckBox = new CheckBox(text = "Cutoff") {
    selected = true
    onAction = (e: ActionEvent) => {
      redraw()
    }
  }
  val (nrows, ncols) = (4, 8)
  var histograms: Vector[NormalizedHistogram] = _

  init()

  private def init() = {
    fetchHistograms()
    redraw()
  }

  private def fetchHistograms() = {
    val histogramBuffer = new ArrayBuffer[NormalizedHistogram]
    for (no <- 0 until nrows * ncols) {
      if (histogramIterator.hasNext)
        histogramBuffer.append(histogramIterator.next())
    }
    histograms = histogramBuffer.toVector
  }

  private def redraw(): Unit = {
    val scenePane = drawScene()
    val scrollPane = new ScrollPane with SceneScrollPane {
      bindSize(HistogramScene.this)
    }
    content = scrollPane
    scrollPane.content = scenePane
  }

  private def reset() = {
    histogramIterator = new VaultHistogramIterator
    init()
  }

  private def drawScene(): Pane = {
    var no = 0
    new GridPane {
      vgap = 2.0
      hgap = 2.0
      padding = Insets(1)
      for {
        row <- 0 until nrows
        col <- 0 until ncols
      } {
        if (histograms.length > no) {
          val node = drawHistogram(histograms(no))
          add(node, col, row)
        }
        no += 1
      }

      val hbox = new HBox() {
        padding = Insets(10)
      }
      hbox.children.append(normalCheckBox)
      hbox.children.append(cdfCheckBox)
      hbox.children.append(cutoffCheckBox)
      val resetButton = new Button("Restart") {
        onAction = (e: ActionEvent) => {
          reset()
        }
      }
      hbox.children.add(resetButton)
      // add(resetButton, 0, nrows)
      if (histogramIterator.hasNext) {
        val nextButton = new Button("Next") {
          onAction = (e: ActionEvent) => {
            init()
          }
        }
        hbox.children.add(nextButton)
      }
      add(hbox, 0, nrows, 2, 4)
    }
  }

  private def drawHistogram(histogram: NormalizedHistogram): Node = {
    val showNormal = normalCheckBox.selected.value
    val showCutoff = cutoffCheckBox.selected.value

    val chart = if (cdfCheckBox.selected.value)
      histogram.plotCdf(width = 250, height = 250, legend = "", fontSize = 5,
        showNormal = showNormal, showClassifierCutoff = showCutoff)
    else
      histogram.plot(width = 251, height = 250, legend = "", fontSize = 5,
        showNormal = showNormal, showClassifierCutoff = showCutoff)
    val swingNode = chartNode(chart)
    new GridPane {
      vgap = 2.0
      hgap = 2.0
      padding = Insets(1)

      var y = 0
      add(swingNode, 0, y)
      y += 1
      val titleLabel = new Label(StringUtils.abbreviateMiddle(histogram.title, "...", 30))
      titleLabel.setTooltip(new Tooltip(text = histogram.title))
      add(titleLabel, 0, y)
      y += 1
      val (max, min) = histogram.dataInBounds()
      val infoLabel = new Label(f"min $min%.2f max $max%.2f size ${histogram.dataIn.length}%d")
      add(infoLabel, 0, y)
      y += 1
    }
  }
}

/**
  * A trait to handle scrollpanes properly
  */
trait SceneScrollPane extends ScrollPane {
  hbarPolicy = ScrollBarPolicy.AsNeeded
  vbarPolicy = ScrollBarPolicy.AsNeeded
  //prefWidth = 1000
  //prefHeight = 800
  maxHeight = 2000
  maxWidth = 4000

  // after the pane is drawn scroll to top left
  private val kf = KeyFrame(Duration(1000), onFinished = {
    (e: ActionEvent) =>
      hvalue = 0.0
      vvalue = 0.0
  })
  Platform.runLater(new Timeline(1.0, Seq(kf)).play())

  /**
    * Size the scrollpane to the parent
    *
    * @param parent parent scene
    */
  def bindSize(parent: Scene): Unit = {
    prefWidth.bind(parent.widthProperty)
    prefHeight.bind(parent.heightProperty)
  }
}