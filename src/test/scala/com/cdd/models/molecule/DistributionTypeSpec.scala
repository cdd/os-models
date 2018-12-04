package com.cdd.models.molecule

import com.cdd.models.universalmetric.DistributionType
import com.cdd.models.utils.Util
import org.scalatest.{FunSpec, Matchers}

class DistributionTypeSpec extends FunSpec with Matchers {

  describe("Reading Chagas SD file and exploring activity data") {

    val fileName = Util.getProjectFilePath("data/continuous/Chagas.sdf.gz").getAbsolutePath
    val tableBuilder = new CdkDataTableBuilder(fileName)
    val values = tableBuilder.doublePropertyValues("IC50_uM")
    val distribution = new DistributionType(values.get.flatten)
    val (isNormal, normalProb) = distribution.isNormal()
    val logDistribution = distribution.toLogDistribution().get
    val (isLogNormal, logNormalProb) = logDistribution.isNormal()

    it("should not be a normal distribution") {
      isNormal should be(false)
      normalProb should be(0.2 +- 0.05)
    }
    it("should not be a log normal distribution") {
      isLogNormal should be(true)
      logNormalProb should be(0.95 +- 0.05)
    }

    DistributionType.histogram("/tmp/IC50_uM.png", distribution.data)
    DistributionType.histogram("/tmp/IC50_uM_dist.png", logDistribution.data)
  }
}
