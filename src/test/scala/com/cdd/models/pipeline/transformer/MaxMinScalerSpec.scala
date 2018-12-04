package com.cdd.models.pipeline.transformer

import com.cdd.models.datatable.DataTable
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.scalatest.{FunSpec, Matchers}

class MaxMinScalerSpec extends FunSpec with Matchers {

  describe("Normalizing features") {

    var dt = DataTable.loadProjectFile("data/continuous/Caco2.csv.gz").select("rdkit_descriptors").filterNull()
    dt = new FilterSameFeatureValues().setFeaturesColumn("rdkit_descriptors").fit(dt).transform(dt)
    val normalizer = new MaxMinScaler().setFeaturesColumn("uniqueFeatures").setOutputColumn("normalizedDescriptors")
    val model = normalizer.fit(dt)
    val normDt = model.transform(dt)
    val normFeatures = normDt.column("normalizedDescriptors").toVector()
    val originalFeatures = normDt.column("uniqueFeatures").toVector()

    (0 until normFeatures(0).length()).foreach { index =>
      val values = normFeatures.map {
        _.toArray()(index)
      }
      val originalValues = originalFeatures.map {
        _.toArray()(index)
      }
      val stats = new DescriptiveStatistics(originalValues.toArray)

      originalValues.zip(values).zipWithIndex.foreach { case ((o, v), index2) =>
        val pred = (o - stats.getMin) / (stats.getMax - stats.getMin)

        it(s"feature ${index} value ${index2} should be ${pred} mean") {
          pred should be(v +- 0.001)
        }
        it(s"feature ${index} value ${index2} should be between 0 and 1") {
          v should be(0.5 +- 0.5)
        }
      }

    }
  }
}
