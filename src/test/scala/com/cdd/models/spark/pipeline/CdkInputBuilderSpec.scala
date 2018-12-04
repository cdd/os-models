package com.cdd.models.spark.pipeline

import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.Configuration
import com.cdd.spark.pipeline.CdkInputBuilder
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gareth on 5/16/17.
  */
class CdkInputBuilderSpec extends FunSpec with Matchers {

  describe("Building a data frame of CDK fingerprints for a set of compounds") {

    describe("Building a dataframe containing sparse fingerprints") {
      val builder = new CdkInputBuilder("data/continuous/Chagas.sdf.gz")
        .setActivityField("IC50_uM")
        .build(Configuration.sparkSession)
        .addFingerprintColumn(CdkFingerprintClass.ECFP6, FingerprintTransform.Sparse)
      var df = builder.getDf()
      df = DataFrameOps.filterForEstimator(df, builder.firstFingerprintColumn())


      it("should have created a dataframe of 743 rows") {
        df.count() should be(743)
      }

      it("the dataframe should contain sparse vectors or length 3968") {
        df.first().getAs("features").asInstanceOf[SparseVector].size should be(3968)
      }
    }

    describe("Building a dataframe containing dense fingerprints") {
      val builder = new CdkInputBuilder("data/continuous/Chagas.sdf.gz")
        .setActivityField("IC50_uM")
        .build(Configuration.sparkSession)
        .addFingerprintColumn(CdkFingerprintClass.ECFP6, FingerprintTransform.Fold, 200)
      var df = builder.getDf()
      df = DataFrameOps.filterForEstimator(df, builder.firstFingerprintColumn())

      it("should have created a dataframe of 743 rows") {
        df.count() should be(743)
      }

      it("the dataframe should contain dense vectors or length 200") {
        df.first().getAs("features").asInstanceOf[DenseVector].size should be(200)
      }
    }
  }


}
