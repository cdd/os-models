package com.cdd.models.pipeline

import java.awt.Font
import java.lang

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.ClassificationEvaluator.ClassificationStats
import com.cdd.models.utils.HasLogging
import org.apache.commons.math3.stat.correlation.{PearsonsCorrelation, SpearmansCorrelation}
import org.apache.log4j.Logger
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.{BitmapEncoder, XYChart, XYChartBuilder}
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.internal.chartpart.Chart
import org.knowm.xchart.style.Styler.LegendPosition
import org.knowm.xchart.style.markers.SeriesMarkers

import collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object Evaluator {
  def setChartFont(fontSize: Int, chart: XYChart): Unit = {
    if (fontSize > -1) {
      val font = new Font("Helvetica", Font.PLAIN, fontSize)
      chart.getStyler
        .setBaseFont(font)
      chart.getStyler.setAxisTickLabelsFont(font)
      chart.getStyler.setChartTitleFont(font)
      chart.getStyler.setAxisTitleFont(font)
      chart.getStyler.setLegendFont(font)
    }
  }
}

@SerialVersionUID(5340054146621277292L)
abstract class Evaluator extends HasParameters with HasPredictionColumn with HasLabelColumn with HasLogging {

  def evaluate(dataTable: DataTable): Double

  val metric = new Parameter[String](this, "metric", "The metric used to evaluate the prediction")

  def setMetric(value: String): this.type = setParameter(metric, value)

  setDefaultParameter(metric, "Unknown Metric")

  def labelsAndPredictions(dataTable: DataTable) = {
    val labels = dataTable.column(getParameter(labelColumn)).toDoubles()
    val predictions = dataTable.column(getParameter(predictionColumn)).toDoubles()

    assert(labels.length == predictions.length)
    (labels, predictions)
  }

  def isLargerBetter(): Boolean

  def chart(dataTable: DataTable, title: String, width: Int, height: Int, fontSize: Int): XYChart

  def plot(file: String, dataTable: DataTable, title: String): Unit


  def gridPlot(file: String, dataTables: Vector[DataTable], titles: Vector[String], cols: Int = 3, width: Int = 500, height: Int = 500, fontSize: Int = -1): Unit = {

    var charts = dataTables.zip(titles).map { case (dt, t) =>
      chart(dt, t, width, height, fontSize)
    }

    var rows = dataTables.length / cols
    if (dataTables.length % cols > 0) {
      rows += 1
      // need to pad grid with dummy charts or bitmap save fails
      for (_ <- 0 to dataTables.length % cols) {
        val chart = new XYChartBuilder().xAxisTitle("X").yAxisTitle("Y").width(width).height(height).build()
        charts = charts :+ chart
      }
    }

    BitmapEncoder.saveBitmap(charts.asJava.asInstanceOf[java.util.List[Chart[_, _]]], rows, cols, file, BitmapFormat.PNG)
  }

}

@SerialVersionUID(-7853524844130442988L)
class RegressionEvaluator(override val uid: String) extends Evaluator {
  def this() = this(Identifiable.randomUID("regEval"))

  setDefaultParameter(metric, "rmse")

  override def evaluate(dataTable: DataTable): Double = {

    require(List("rmse", "mae").contains(getParameter(metric)))

    getParameter(metric) match {
      case "rmse" => rmse(dataTable)
      case "mae" => mae(dataTable)
      case _ => throw new IllegalArgumentException(s"Unknown regression evaluation metric ${metric}")
    }
  }

  def rmse(dataTable: DataTable): Double = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    val ssd = labels.zip(predictions).foldLeft(.0) { (s, el) => s + (el._1 - el._2) * (el._1 - el._2) }
    Math.sqrt(ssd / labels.length.toDouble)
  }

  def mae(dataTable: DataTable): Double = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    val smad = labels.zip(predictions).foldLeft(.0) { (s, el) => s + Math.abs(el._1 - el._2) }
    smad / labels.length.toDouble
  }

  def correlation(dataTable: DataTable): Double = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    (new PearsonsCorrelation).correlation(labels.toArray, predictions.toArray)
  }

  def spearmansCorrelation(dataTable: DataTable): Double = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    (new SpearmansCorrelation).correlation(labels.toArray, predictions.toArray)
  }

  override def chart(dataTable: DataTable, title: String = "Regression Plot", width: Int = 800, height: Int = 800,
                     fontSize: Int = -1): XYChart = {
    chart(dataTable, title, width, height, fontSize, false)
  }

  def chart(dataTable: DataTable, title: String, width: Int, height: Int,
            fontSize: Int, showLine: Boolean): XYChart = {
    val labels = dataTable.column(getParameter(labelColumn)).toDoubles()
    val predictions = dataTable.column(getParameter(predictionColumn)).toDoubles()
    val metricVal = evaluate(dataTable)
    val corr = correlation(dataTable)
    val label = f"${getParameter(metric)} $metricVal%.2f [C $corr%.2f]"
    val chart = new XYChartBuilder().width(width).height(height).title(title).xAxisTitle("Label").yAxisTitle("Prediction").build()
    Evaluator.setChartFont(fontSize, chart)

    chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter)
    chart.getStyler().setLegendPosition(LegendPosition.InsideNW)
    val series = chart.addSeries(label, labels.toArray, predictions.toArray)
    series.setMarker(SeriesMarkers.CIRCLE)

    if (showLine) {
      val labelRange = Array(labels.min, labels.max)
      val series2 = chart.addSeries("y=x", labelRange, labelRange)
      series2.setXYSeriesRenderStyle(XYSeriesRenderStyle.Line)
      series2.setMarker(SeriesMarkers.NONE)
    }

    chart
  }

  override def plot(file: String, dataTable: DataTable, title: String = "Regression Plot"): Unit = {
    val c = chart(dataTable, title)
    BitmapEncoder.saveBitmap(c, file, BitmapFormat.PNG)
  }

  override def isLargerBetter(): Boolean = false
}

object ClassificationEvaluator {

  class ClassificationStats(val tp: Int, val tn: Int, val fp: Int, val fn: Int) {

    def precision: Double = {
      val p = tp + fp
      if (p == 0) 0.0 else tp.toDouble / p.toDouble
    }

    // recall or true positive rate or sensitivity
    def recall: Double = {
      val p = tp + fn
      if (p == 0) 0.0 else tp.toDouble / p.toDouble
    }

    // specificity or true negative rate
    def specificity: Double = {
      val p = tn + fp
      if (p == 0) 0.0 else tn.toDouble/p.toDouble
    }

    def ba: Double = {
      (specificity + recall) / 2.0
    }

    def f1: Double = {
      if (recall == 0.0 || precision == 0.0) 0.0 else 2.0 * precision * recall / (precision + recall)
    }

    def scaledCost(fpCost: Double=1.0, fnCost: Double=10.0) : Double= {
      val n = tp + tn + fp + fn
      (fp.toDouble + fpCost + fn.toDouble * fnCost) / n.toDouble
    }
  }

}

@SerialVersionUID(-5916538261292036782L)
class ClassificationEvaluator(override val uid: String) extends Evaluator with HasProbabilityColumn {

  def this() = this(Identifiable.randomUID("classEval"))

  setDefaultParameter(metric, "accuracy")

  override def evaluate(dataTable: DataTable): Double = {

    require(List("accuracy", "recall", "precision", "f1", "rocAuc").contains(getParameter(metric)))

    getParameter(metric) match {
      case "accuracy" => accuracy(dataTable)
      case "precision" => statistics(dataTable).precision
      case "recall" => statistics(dataTable).recall
      case "f1" => statistics(dataTable).f1
      case "rocAuc" => rocAuc(dataTable)
      case _ => throw new IllegalArgumentException(s"Unknown classification evaluation metric ${metric}")
    }
  }

  def accuracy(dataTable: DataTable): Double = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    val nCorrect = labels.zip(predictions).foldLeft(0) { (s, el) => if (el._1 == el._2) s + 1 else s }
    nCorrect.toDouble / labels.length.toDouble
  }

  def statistics(labels: Vector[Double], predictions: Vector[Double]): ClassificationStats = {
    var tp = 0
    var tn = 0
    var fp = 0
    var fn = 0

    labels.zip(predictions).foreach { case (l, p) =>
      if (l == 1 && p == 1) tp += 1
      if (l == 0 && p == 0) tn += 1
      if (l == 0 && p == 1) fp += 1
      if (l == 1 && p == 0) fn += 1
    }

    assert(labels.size == tp + tn + fp + fn)
    new ClassificationStats(tp, tn, fp, fn)
  }

  def statistics(dataTable: DataTable): ClassificationStats = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    statistics(labels, predictions)
  }

  def rocData(dataTable: DataTable): Vector[(Double, Double)] = {
    val (labels, predictions) = labelsAndPredictions(dataTable)
    val probabilities = dataTable.column(getParameter(probabilityColumn)).toDoubles()
    val nPositive = labels.filter(_ == 1.0).length
    val nNegative = labels.filter(_ == 0.0).length
    require(nPositive + nNegative == dataTable.length)

    // see https://www.epeter-stats.de/roc-curves-and-ties/ for issues round this treatment of ties
    val sortedLabels = labels.zip(probabilities).sortBy(-_._2)
    val coords = sortedLabels.foldLeft(ArrayBuffer[Tuple3[Double, Double, Double]]()) { case (c, (label, prob)) =>
      val (x, y, lastProb) = if (c.length > 0) c.last else (.0, .0, .0)
      val next = if (label == 1.0) (x, y + 1, prob) else (x + 1, y, prob)
      // collapse ties to a single point
      if (prob == lastProb)
        c.remove(c.size - 1)
      c += next
      c
    }
    val rocData = coords.map { case (x, y, _) => (x / nNegative.toDouble, y / nPositive.toDouble) }.toVector
    rocData
  }

  def rocAuc(dataTable: DataTable): Double = {
    rocAuc(rocData(dataTable))
  }

  def rocAuc(rocData: Vector[(Double, Double)]): Double = {
    val (area, _, _) = rocData.foldLeft(0.0, 0.0, 0.0) {
      case ((area, x1, y1), (x2, y2)) =>
        val trapeziod = (x2 - x1) * (y1 + y2) / 2.0
        (area + trapeziod, x2, y2)
    }
    area
  }

  override def plot(file: String, dataTable: DataTable, title: String): Unit = {
    plot(file, rocData(dataTable), title)

  }

  override def chart(dataTable: DataTable, title: String = "ROC Curve", width: Int = 800, height: Int = 800,
                     fontSize: Int = -1): XYChart = {
    chart(rocData(dataTable), title, width, height, fontSize)
  }

  def chart(rocData: Vector[(Double, Double)], title: String, width: Int, height: Int, fontSize: Int): XYChart = {

    val chart = new XYChartBuilder().width(width).height(height).title(title).xAxisTitle("FPR").yAxisTitle("TPR").build()

    chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line)
    chart.getStyler().setLegendPosition(LegendPosition.InsideSE)

    Evaluator.setChartFont(fontSize, chart)

    val (fpr, tpr) = rocData.unzip
    val auc = rocAuc(rocData)
    val aucStr = f"$auc%.2f"
    val series = chart.addSeries(s"ROC AUC ${aucStr}", fpr.toArray, tpr.toArray)
    series.setMarker(SeriesMarkers.NONE)

    val labelRange = Array(0, 1)
    val series2 = chart.addSeries("y=x", labelRange, labelRange)
    series2.setXYSeriesRenderStyle(XYSeriesRenderStyle.Line)
    series2.setMarker(SeriesMarkers.NONE)
    chart
  }

  def plot(file: String, rocData: Vector[(Double, Double)], title: String = "ROC Curve"): Unit = {

    val c = chart(rocData, title, 800, 800, -1)
    BitmapEncoder.saveBitmap(c, file, BitmapFormat.PNG);
  }

  override def isLargerBetter(): Boolean = true

}