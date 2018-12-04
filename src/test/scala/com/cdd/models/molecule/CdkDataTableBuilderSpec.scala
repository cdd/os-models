package com.cdd.models.molecule

import com.cdd.models.universalmetric.DistributionType
import com.cdd.models.utils.Util
import org.scalatest.{FunSpec, Matchers}

class CdkDataTableBuilderSpec extends FunSpec with Matchers {

  describe("Reading an SD file and extracting properties") {
    val fileName = Util.getProjectFilePath("data/AZ_ADME.sdf.gz").getAbsolutePath
    val tableBuilder = new CdkDataTableBuilder(fileName)

    val properties = tableBuilder.allProperties().filter {
      _ match {
        case s if s.endsWith(": Score") => false
        case s if s.endsWith(": Applicability") => false
        case s if s.endsWith(": Maximum similarity") => false
        case _ => true
      }
    } toVector
    val doubleData = properties
      .map { p => (p, tableBuilder.doublePropertyValues(p)) }
      .filter { case (_, values) => values != None }
      .map { case (p, values) => (p, values.get) }

    info(s"properties are ${properties.mkString("|")}")
    doubleData.foreach { case (p, values) =>
      val nValues = values.count(_ != None)
      info(s"${p} no values ${nValues}")

      val dt = new DistributionType(values.flatten)
      info(dt.info())
    }

    val values = tableBuilder.doublePropertyValues("AZ Human Microsomal Intrinsic Clearance: Human microsomal intrinsic clearance (mL.min-1.g-1)")
    val distribution = new DistributionType(values.get.flatten)
    val logDistribution = distribution.toLogDistribution()
    DistributionType.histogram("/tmp/clearance_dist.png", distribution.data)
    DistributionType.histogram("/tmp/clearance_log_dist.png", logDistribution.get.data)

  }
}
