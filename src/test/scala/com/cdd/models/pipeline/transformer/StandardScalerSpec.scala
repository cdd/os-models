package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable.DataTable
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.scalatest.{FunSpec, Matchers}

class StandardScalerSpec extends FunSpec with Matchers {

  describe("Normalizing features") {

    val dt = DataTable.loadProjectFile("data/continuous/Chagas.csv.gz").select("rdkit_descriptors").filterNull()
    val normalizer = new StandardScaler().setFeaturesColumn("rdkit_descriptors").setOutputColumn("normalizedDescriptors")
    val normDt = normalizer.fit(dt).transform(dt)
    val normFeatures = normDt.column("normalizedDescriptors").toVector()
    var nVariableFeatures = 0
    var nConstantFeatures = 0

    (0 until normFeatures(0).length()).foreach { index =>
      val values = normFeatures.map {
        _.toArray()(index)
      }
      val stats = new DescriptiveStatistics(values.toArray)
      it(s"feature ${index} should have a 0 mean") {
        stats.getMean should be(0.0 +- 0.001)
      }
      if (stats.getMax == 0 && stats.getMin == 0) {
        nConstantFeatures += 1
        it(s"feature ${index} should have a SD of 0") {
          stats.getStandardDeviation should be(0.0 +- 0.001)
        }
      } else {
        nVariableFeatures += 1
        it(s"feature ${index} should have a SD of 1") {
          stats.getStandardDeviation should be(1.0 +- 0.001)
        }
      }
    }

    info(s"N variable features is ${nVariableFeatures} N constant features ${nConstantFeatures}")
    it("should have 106 variable features") {
      nVariableFeatures should be(106)
    }
    it ("should have 6 constant features") {
      nConstantFeatures should be(9)
    }
  }
}
