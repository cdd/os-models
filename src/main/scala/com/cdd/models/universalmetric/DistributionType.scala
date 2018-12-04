package com.cdd.models.universalmetric

import com.cdd.models.bae.AssayDirection
import com.cdd.models.bae.AssayDirection.AssayDirection
import com.cdd.models.utils.HasLogging
import net.sourceforge.jdistlib.disttest.{DistributionTest, NormalityTest}
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle
import org.knowm.xchart.style.Styler.LegendPosition
import org.knowm.xchart.style.markers.SeriesMarkers
import org.knowm.xchart.{BitmapEncoder, XYChart, XYChartBuilder}

object DistributionType extends HasLogging {

  /**
    * Performs Shapiro Wilk Normalization test on a distribution
    *
    * @param data
    * @return a tuple of (probability, statistic)
    */
  def normalTest(data: Vector[Double]): (Double, Double) = {
    val sortedData = data.sorted.toArray
    val statistic = NormalityTest.shapiro_wilk_statistic(sortedData)
    val probability = NormalityTest.shapiro_wilk_pvalue(statistic, sortedData.length)
    (probability, statistic)
  }

  /**
    * Performs Hartigan's dip test on a distribution
    *
    * @param data
    * @return a tuple of (probability, statistic)
    */
  def dipTest(data: Vector[Double]): (Double, Double) = {
    val sortedData = data.sorted.toArray
    val result = DistributionTest.diptest_presorted(data.toArray)
    val statistic = result(0)
    val probability = result(1)

    (probability, statistic)
  }

  /**
    * Creates an XChart histogram for a distribution
    *
    * @param data   distribution
    * @param legend legend for chart
    * @param nBins  number of bins in the histogram
    * @return chart
    */
  def histogramChart(data: Vector[Double], legend: String = "Property distribution", nBins: Int = 10): XYChart = {

    val chart = new XYChartBuilder().width(800).height(800).title(legend).xAxisTitle("Values").yAxisTitle("Count").build()

    chart.getStyler.setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line)
    chart.getStyler.setLegendPosition(LegendPosition.InsideNW)

    val max = data.max
    val min = data.min
    val bin_width = (max - min) / nBins.toDouble

    val bins = data.map { v => ((v - min) / bin_width).toInt }
    val binCounts = bins.groupBy((b) => b).mapValues(_.length)

    var ys = (bins.min to bins.max).map(binCounts.getOrElse(_, 0).toDouble)
      .flatMap(v => List(v.toDouble, v.toDouble, 0)).toList
    var xs = (bins.min to bins.max)
      .flatMap {
        (bin) =>
          val nextX = min + bin_width * (bin.toDouble + 1)
          List(min + bin_width * bin.toDouble, nextX, nextX)
      }
      .toList

    ys = 0.0 :: (ys :+ 0.0)
    xs = min :: (xs :+ max + bin_width)
    val series = chart.addSeries("Dist", xs.toArray, ys.toArray)
    series.setMarker(SeriesMarkers.NONE)
    chart
  }

  /**
    * Create a PNG file containing a frequency histogram for a distribution
    *
    * @param file   file to save image
    * @param data   distribution
    * @param legend legend for chart
    * @param nBins  number of bins in the histogram
    */
  def histogram(file: String, data: Vector[Double], legend: String = "Property distribution", nBins: Int = 10): Unit = {
    val chart = histogramChart(data, legend, nBins)
    BitmapEncoder.saveBitmap(chart, file, BitmapFormat.PNG)
  }
}

/**
  * A class to model real values distributions
  *
  * @param inputData
  */
class DistributionType(val inputData: Vector[Double]) extends HasLogging {

  val data: Vector[Double] = inputData.sorted

  /**
    * @return true if we have a meaningful number of values
    */
  def hasEnoughValues(): Boolean = {
    data.length > 50
  }

  /**
    * @return true if this distribution is likely categorical
    */
  def isCategorical(): Boolean = {
    data.distinct.length < 10
  }

  /**
    * Requires that the distribution be categorical
    *
    * @return the number of categories in the data
    */
  def noCategories(): Int = {
    require(isCategorical())
    data.distinct.length
  }

  /**
    * Determine probability that data is normal
    *
    * @return
    */
  def isNormal(): (Boolean, Double) = {
    val (statistic, probability) = DistributionType.normalTest(data)
    logger.debug(s"Normal distribution test statistic ${statistic} probability ${probability}")
    (probability > 0.9, probability)
  }

  /** *
    * Convert this data to a log distribution
    *
    * @return Log distribution or None if any value is 0 or less
    */
  def toLogDistribution(): Option[DistributionType] = {
    if (data.exists(_ <= .0))
      return None
    val logData = data.map(Math.log10)
    Some(new DistributionType(logData))
  }

  /**
    * @return Return informational string
    */
  def info(): String = {
    val sb = new StringBuilder()
    sb ++= s"No values ${data.length}"
    if (!hasEnoughValues()) {
      sb ++= " Not enough values"
    }
    else if (isCategorical()) {
      sb ++= s" categorical- no categories: ${noCategories()}"
    }
    else {
      val (normal, normalProb) = isNormal()
      sb ++= s" normal dist $normal [P $normalProb]"
      val (dip, dipProb) = isMultinomial()
      sb ++= s" multinomial dist dip test $dip [P $dipProb]"
    }
    sb.toString()
  }

  /**
    * Using a dip test return probability that data is multinomial
    *
    * @return
    */
  def isMultinomial(): (Boolean, Double) = {
    val result = DistributionTest.diptest_presorted(data.toArray)
    val statistic = result(0)
    val probability = result(1)

    (probability > 0.9, probability)
  }

  def inactiveValue(): Option[Double] = {
    val maxValue = data.max
    val maxCount = data.filter(_ == maxValue).length
    if (maxCount > 10 && maxCount * 20 > data.length) {
      Some(maxValue)
    } else {
      None
    }
  }

  def toActiveCategory(percentActive: Double = 10.0, direction: Option[AssayDirection] = None)
  : (Double, AssayDirection) = {

    val statistics = new DescriptiveStatistics(data.toArray)
    val skewness = statistics.getSkewness
    val mean = statistics.getMean
    val median = statistics.getPercentile(50.0)

    if (skewness > 0) {
      if (mean <= median)
        logger.debug("Right skewed distribution with mean less than median")
    }
    else if (skewness < 0) {
      if (mean >= median)
        logger.debug("Left skewed distribution with mean greater than median")
    }

    val assayDirection = direction match {
      case Some(ad) => ad
      case None =>
        if (skewness > 0) AssayDirection.UP else AssayDirection.DOWN
    }

    val threshold = assayDirection match {
      case AssayDirection.UP => statistics.getPercentile(100 - percentActive)
      case AssayDirection.DOWN => statistics.getPercentile(percentActive)
      case _ => throw new IllegalArgumentException
    }

    (threshold, assayDirection)
  }
}
