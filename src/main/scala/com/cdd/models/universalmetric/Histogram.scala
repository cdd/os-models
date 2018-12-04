package com.cdd.models.universalmetric

import java.awt.{BasicStroke, Color, Font}
import java.util

import com.cdd.models.bae.AssayDirection
import com.cdd.models.bae.AssayDirection.AssayDirection
import com.cdd.models.stats.{AdaptiveSamplePointKde, GaussianKde, Kde, Solver}
import com.cdd.models.universalmetric.ClassificationPointType.ClassificationPointType
import com.cdd.models.universalmetric.DerivativePointType.DerivativePointType
import com.cdd.models.universalmetric.HistogramDataType.HistogramDataType
import com.cdd.models.utils.Util
import org.apache.commons.lang3.StringUtils
import org.apache.commons.math3.analysis.function.Gaussian
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.knowm.xchart
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler.LegendPosition
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.{SwingWrapper, XYChart, XYChartBuilder, XYSeries}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class Histogram(val data: Vector[Double]) {
  @transient
  lazy val descriptiveStatistics: DescriptiveStatistics = new DescriptiveStatistics(data.toArray)

  def toFrequency(nBins: Int, normalize: Boolean = false): Vector[Double] = {
    val (min, max) = bounds()
    val barWidth = (max - min) / nBins.toDouble
    val bins = ArrayBuffer.fill(nBins)(0.0)
    data.foreach { v =>
      var barNo = ((v - min) / barWidth).toInt
      if (barNo == nBins) {
        barNo = nBins - 1
      }
      bins(barNo) += 1.0
    }

    if (normalize) {
      val barSum = bins.sum * barWidth
      bins.map(_ / barSum).toVector
    } else {
      bins.toVector
    }
  }

  def toCdf(): Vector[(Double, Double)] = {
    val sum = data.sum
    val (min, max) = bounds()
    val s = data.sorted
    s.scanLeft((0.0, 0.0)) { case ((_, s), v) =>
      (v, v + s)
    }.map { case (x, y) => ((x - min) / (max - min), y / sum) }
  }

  def bounds(): (Double, Double) = {
    (data.min, data.max)
  }

  def normalizeRange(): Histogram = {
    val (min, max) = bounds()
    require(max > min)
    val newData = data.map { v =>
      (v - min) / (max - min)
    }
    new Histogram(newData)
  }

  def stddevAndMean(): (Double, Double) = {
    (descriptiveStatistics.getStandardDeviation, descriptiveStatistics.getMean)
  }

  def skew(): Double = {
    descriptiveStatistics.getSkewness
  }

  def kde(nPoints: Int = 100, normalize: Boolean = false): KdeCurve = {
    val kde = new AdaptiveSamplePointKde(data)
    //new GaussianKde(data)
    val (min, max) = bounds()
    val spacing = (max - min) / (nPoints.toDouble - 1.0)

    val maxima = ArrayBuffer[(Double, Double)]()
    val minima = ArrayBuffer[(Double, Double)]()
    val inflectionPoints = ArrayBuffer[(Double, Double)]()
    var previousFirstD = 0.0
    var previousSecondD = 0.0
    val (kdePoints, cdf) =
      kde.fullGrid(nPoints, None, None).zipWithIndex.map { case ((x, y, firstD, secondD, cdfV), p) =>

        if (p > 0) {
          if (firstD >= 0.0 && previousFirstD < 0) {
            minima.append((x, y))
          }
          else if (firstD <= 0.0 && previousFirstD > 0) {
            maxima.append((x, y))
          }
          if ((secondD >= 0.0 && previousSecondD < 0) ||
            (secondD <= 0.0 && previousSecondD > 0)) {
            inflectionPoints.append((x, y))
          }
        }

        previousFirstD = firstD
        previousSecondD = secondD
        ((x, y), cdfV)
      }.unzip

    new KdeCurve(kde, kdePoints, cdf, maxima.toVector, minima.toVector, inflectionPoints.toVector)

  }
}

object DerivativePointType extends Enumeration {
  type DerivativePointType = Value
  val Maximum, Minimum, InflectionPoint = Value
}

case class DerivativePoint(pointType: DerivativePointType, x: Double, y: Double)

class KdeCurve(val kde: Kde, val points: Vector[(Double, Double)], val cdf: Vector[Double],
               val maxima: Vector[(Double, Double)],
               val minima: Vector[(Double, Double)], val inflectionPoints: Vector[(Double, Double)]) {

  def derivativePoints(): Vector[DerivativePoint] = {
    val min = minima.map { case (x, y) => DerivativePoint(DerivativePointType.Minimum, x, y) }
    val max = maxima.map { case (x, y) => DerivativePoint(DerivativePointType.Maximum, x, y) }
    val inf = inflectionPoints.map { case (x, y) => DerivativePoint(DerivativePointType.InflectionPoint, x, y) }

    (min ++ max ++ inf).sortBy(_.x)
  }

  def yAtPercentile(percentile: Double): Double = {
    0.0
  }
}

object HistogramDataType extends Enumeration {
  type HistogramDataType = Value
  val RAW, PIC50 = Value
}

object DerivativePointsOps {


  def findMinimaInList(derivativePoints: Vector[DerivativePoint]): Vector[(DerivativePoint, DerivativePoint, DerivativePoint)] = {
    var points =
      None +:
        derivativePoints
          .filter(p => p.pointType == DerivativePointType.Minimum || p.pointType == DerivativePointType.Maximum)
          .map(Some(_))
    points = points :+ None
    points.sliding(3).flatMap {
      case Vector(Some(prev), Some(current), Some(next)) =>
        if (current.pointType == DerivativePointType.Minimum) {
          assert(prev.pointType == DerivativePointType.Maximum)
          assert(next.pointType == DerivativePointType.Maximum)
          Some(prev, current, next)
        }
        else {
          None
        }
      case _ => None
    }.toVector
  }

  def findInflectionPairsInList(derivativePoints: Vector[DerivativePoint]): Vector[(DerivativePoint, DerivativePoint)] = {
    derivativePoints.sliding(2).filter { case Vector(point1, point2) =>
      point1.pointType == DerivativePointType.InflectionPoint &&
        point2.pointType == DerivativePointType.InflectionPoint
    }
      .map { case Vector(p1, p2) => (p1, p2) }
      .toVector
  }

}

object ClassificationPointType extends Enumeration {
  type ClassificationPointType = Value
  val Minimum, Inflection, Proportion, Fixed = Value
}

class ClassificationPoint(val pointType: ClassificationPointType, val x: Double, val assayDirection: AssayDirection)

class NormalizedHistogram(val dataIn: Vector[Double], val nBins: Int = 10, val dataType: HistogramDataType,
                          val title: String = "Frequency Distribution") {
  val histogram: Histogram = new Histogram(dataIn).normalizeRange()
  val bins: Vector[Double] = histogram.toFrequency(nBins, normalize = true)
  val kde: KdeCurve = histogram.kde()

  def dataInBounds(): (Double, Double) = {
    (dataIn.max, dataIn.min)
  }

  def toUnNormalized(x: Double): Double = {
    val (max, min) = dataInBounds()
    min + x * (max - min)
  }

  def findClassificationPoint(normalize: Boolean = false, proportion:Double=0.333): ClassificationPoint = {
    def xVal(x: Double) = {
      if (normalize) toUnNormalized(x) else x
    }

    def xToAssayDirection(x: Double): AssayDirection = {
      val cdf = kde.kde.cdf(x)
      if (cdf >= 0.5) AssayDirection.UP else AssayDirection.DOWN
    }

    val derivativePoints = kde.derivativePoints()
    val mid = Solver.findXforY(kde.kde.cdf, 0.5, 0, 1.0)

    val minima = DerivativePointsOps.findMinimaInList(derivativePoints)
      .filter { case (prev, current, next) =>
        if (kde.kde.proportion(current.x) < .20)
          false
        else if (Math.max(prev.y - current.y, next.y - current.y) < 0.15)
          false
        else if (dataType == HistogramDataType.PIC50 && current.x < mid)
          false
        else
          true
      }.map { case (_, current, _) =>
      current.x
    }

    if (minima.nonEmpty) {
      val x = minima(0)
      return new ClassificationPoint(ClassificationPointType.Minimum, xVal(x), xToAssayDirection(x))
    }

    val inflectionPairs = DerivativePointsOps.findInflectionPairsInList(derivativePoints)
      .filter { case (prev, next) =>
        val grad = Math.abs((prev.y - next.y) / (prev.x - next.x))
        val x = (prev.x + next.x) / 2.0
        if (kde.kde.proportion(x) < .20)
          false
        else if (dataType == HistogramDataType.PIC50 && x < mid)
          false
        else if (grad > 5.0)
          false
        else
          true
      }.map { case (prev, next) =>
      (prev.x + next.x) / 2.0
    }
    if (inflectionPairs.nonEmpty) {
      val x = inflectionPairs(0)
      return new ClassificationPoint(ClassificationPointType.Inflection, xVal(x), xToAssayDirection(x))
    }

    val skew = histogram.skew()
    val p = if (dataType == HistogramDataType.PIC50 || skew >= 0) 1.0 - proportion else proportion
    val x = Solver.findXforY(kde.kde.cdf, p, 0, 1.0)
    val assayDirection = if (skew >= 0) AssayDirection.UP else AssayDirection.DOWN
    new ClassificationPoint(ClassificationPointType.Proportion, xVal(x), assayDirection)
  }

  def plot(width: Int = 800, height: Int = 800, legend: String = null, fontSize: Int = 12,
           showNormal: Boolean = false, showClassifierCutoff: Boolean = true): XYChart = {

    val chartLegend = if (legend == null) title else legend
    val chart = new XYChartBuilder()
      .width(width)
      .height(height)
      // .xAxisTitle("Values")
      //.yAxisTitle("probability")
      .build()
    if (StringUtils.isNotBlank(chartLegend))
      chart.setTitle(chartLegend)

    val font = new Font("Helvetica", Font.PLAIN, fontSize)
    chart.getStyler
      .setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line)
      .setLegendPosition(LegendPosition.InsideNW)
      .setBaseFont(font)
      .setChartTitleFont(font)
      .setLegendVisible(false)
    chart.getStyler.setAxisTickLabelsFont(font)

    val max = histogram.data.max
    val min = histogram.data.min
    val barWidth = (max - min) / nBins.toDouble
    val small = barWidth / 100.0

    var sum = .0
    val (xs, ys) = bins.zipWithIndex.flatMap { case (bin, index) =>
      val x = index.toDouble * barWidth
      val startX = x + small
      val nextX = x + barWidth - small
      sum += bin
      Vector((startX, bin), (nextX, bin))
    }.unzip

    // this required for the line plot
    // ys = 0.0 +: (ys :+ 0.0)
    // xs = min +: (xs :+ max)
    xs.sliding(2).foreach { case Vector(p, n) =>
      require(n > p)
    }
    val series1 = chart.addSeries("Dist", xs.toArray, ys.toArray)
    series1.setXYSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Area)
    series1.setMarker(SeriesMarkers.NONE)
    val color1 = new Color(147, 112, 219, 128)
    series1.setFillColor(color1)
    series1.setLineColor(color1)

    val (xs2, ys2) = kde.points.unzip
    val series2 = chart.addSeries("KDE", xs2.toArray, ys2.toArray)
    series2.setMarker(SeriesMarkers.NONE)
    val color2 = new Color(255, 0, 0, 255)
    series2.setLineColor(color2)

    val (xs3, ys3) = kde.maxima.unzip
    if (xs3.length > 0) {
      val series3 = chart.addSeries("Maxima", xs3.toArray, ys3.toArray)
      series3.setMarker(SeriesMarkers.TRIANGLE_UP)
      series3.setXYSeriesRenderStyle(XYSeriesRenderStyle.Scatter)
      series3.setMarkerColor(Color.BLUE)
    }

    val (xs4, ys4) = kde.minima.unzip
    if (xs4.length > 0) {
      val series4 = chart.addSeries("Minima", xs4.toArray, ys4.toArray)
      series4.setMarker(SeriesMarkers.TRIANGLE_DOWN)
      series4.setXYSeriesRenderStyle(XYSeriesRenderStyle.Scatter)
      series4.setMarkerColor(Color.BLUE)
    }

    val (xs5, ys5) = kde.inflectionPoints.unzip
    if (xs5.length > 0) {
      val series5 = chart.addSeries("Inflection points", xs5.toArray, ys5.toArray)
      series5.setMarker(SeriesMarkers.CIRCLE)
      series5.setXYSeriesRenderStyle(XYSeriesRenderStyle.Scatter)
      series5.setMarkerColor(Color.GREEN)
    }

    if (showNormal) {
      val (sd, m) = histogram.stddevAndMean()
      val gaussian = new Gaussian(m, sd)
      val (xs6, ys6) = kde.points.map { case (x, _) =>
        (x, gaussian.value(x))
      }.unzip
      val series6 = chart.addSeries("Normal", xs6.toArray, ys6.toArray)
      series6.setMarker(SeriesMarkers.NONE)
      val color6 = new Color(0, 255, 0, 255)
      series6.setLineColor(color6)
    }

    if (showClassifierCutoff) {
      val yMax = Math.max(ys.max, ys2.max)
      plotClassifierCutoff(chart, yMax)
    }

    new SwingWrapper[XYChart](chart)
    chart
  }

  private def plotClassifierCutoff(chart: XYChart, yMax: Double) = {
    val classificationPoint = findClassificationPoint()
    val x = classificationPoint.x
    val series7 = chart.addSeries("Classification point", Array(x, x), Array(0, yMax))
    series7.setMarker(SeriesMarkers.NONE)
    val stroke = new BasicStroke(3, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, Array(3.0f), 0);
    series7.setLineStyle(stroke)
    val color7 = classificationPoint.pointType match {
      case ClassificationPointType.Minimum => Color.ORANGE
      case ClassificationPointType.Inflection => Color.YELLOW
      case ClassificationPointType.Proportion => new Color(165, 42, 42)
      case _ => throw new IllegalArgumentException
    }
    series7.setLineColor(color7)
  }

  def plotCdf(width: Int = 800, height: Int = 800, legend: String = null, fontSize: Int = 12,
              showNormal: Boolean = false, showClassifierCutoff: Boolean = true): XYChart = {
    val chartLegend = if (legend == null) title else legend
    val chart = new XYChartBuilder()
      .width(width)
      .height(height)
      // .xAxisTitle("Values")
      //.yAxisTitle("probability")
      .build()
    if (StringUtils.isNotBlank(chartLegend))
      chart.setTitle(chartLegend)

    val font = new Font("Helvetica", Font.PLAIN, fontSize)
    chart.getStyler
      .setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line)
      .setLegendPosition(LegendPosition.InsideNW)
      .setBaseFont(font)
      .setLegendVisible(false)
    chart.getStyler.setAxisTickLabelsFont(font)

    val dataCdf = histogram.toCdf()
    val (xs, ys) = dataCdf.sliding(2).flatMap { case Vector((x1, y1), (x2, y2)) =>
      Vector((x1, y1), (x2, y1), (x2, y2))
    }.toVector.unzip
    val series1 = chart.addSeries("CDF", xs.toArray, ys.toArray)
    series1.setMarker(SeriesMarkers.NONE)
    val color1 = new Color(0, 0, 255, 255)
    series1.setLineColor(color1)

    val (xs2, ys2) = kde.points.zip(kde.cdf).map { case ((x, y), c) => (x, c) }.unzip
    val series2 = chart.addSeries("KDE PDF", xs2.toArray, ys2.toArray)
    series2.setMarker(SeriesMarkers.NONE)
    val color2 = new Color(255, 0, 0, 255)
    series2.setLineColor(color2)

    // empirical check of the CDF- not we are missing the initial probability so this will lie under the analytical CDF
    if (false) {
      val len = kde.points.length.toDouble
      val (xs3, ys3) = kde.points.scanLeft((0.0, 0.0)) { case ((_, s), (x, y)) => (x, s + y / len) }.unzip
      val series3 = chart.addSeries("KDE PDF 2", xs3.toArray, ys3.toArray)
      series3.setMarker(SeriesMarkers.NONE)
      val color3 = new Color(0, 255, 0, 255)
      series3.setLineColor(color3)
    }

    if (showNormal) {
      val (sd, m) = histogram.stddevAndMean()
      val (xs4, ys4) = kde.points.map { case (x, _) =>
        val n = (x - m) / sd
        (x, Util.normalCdf(n))
      }.unzip
      val series4 = chart.addSeries("Normal distribution", xs4.toArray, ys4.toArray)
      series4.setMarker(SeriesMarkers.NONE)
      val color4 = new Color(0, 255, 0, 255)
      series4.setLineColor(color4)
    }

    if (showClassifierCutoff) {
      plotClassifierCutoff(chart, 1.0)
    }

    new SwingWrapper[XYChart](chart)
    chart
  }
}
