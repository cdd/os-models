package com.cdd.models.bae

import com.cdd.models.universalmetric.DistributionType
import com.cdd.models.utils.{HasLogging, Util}
import org.apache.log4j.LogManager
import org.scalatest.{FunSpec, Matchers}

class BaeIc50RegressionModelSpec extends FunSpec with Matchers with HasLogging {

  describe("Building a regression model on a BaeAssay") {
    val uniqueId = "pubchemAID:1055"

    val regression = new BaeIc50RegressionModel(uniqueId)
    val assay = regression.assay
    it ("should have 193 compounds") {
      assay.molecules.size should be(193)
    }
    val columnGroups = assay.findReplicateColumns(assay.ic50Columns).values.toVector.sortBy(_.length)
    it ("should have 2 column groups") {
      columnGroups.size should be(2)
    }
    var test = columnGroups(0)
    it ("should have one mean column") {
      test.size should be(1)
      test(0).name() should be("Ic50_Mean")
    }
    test = columnGroups(1)
    it ("should have one replicate column of size 15") {
      test.size should be(15)
    }
    it ("should have an activity criteria") {
      assay.activityCriteria should not be(None)
    }
    it ("should have an activity criteria with an ic50 column") {
      regression.activityInIc50() should be(true)
    }

    val meanData = new BaeFieldGroup(columnGroups(0).asInstanceOf[Vector[BaeField]])
    val replicateData = new BaeFieldGroup(columnGroups(1).asInstanceOf[Vector[BaeField]])

    val meanLogDistribution = new DistributionType(meanData.averages(true).flatten)
    logger.info(s"mean log distribution info ${meanLogDistribution.info()}")
    val replicateLogDistribution = new DistributionType(replicateData.averages(true).flatten)
    logger.info(s"replicate log distribution info ${replicateLogDistribution.info()}")

    val meanChart = DistributionType.histogramChart(meanLogDistribution.data, "Arithmetic mean")
    //val replicateChart = DistributionType.histogramChart(replicateLogDistribution.data, "Log mean")
    val replicateChart = DistributionType.histogramChart(replicateData.values(true), "Log mean")
    Util.gridPlot("/tmp/assay_10402.png", Vector(meanChart, replicateChart), 2)
    regression.doRegression

    regression.saveResults()
    info("Finished run")
  }
}
