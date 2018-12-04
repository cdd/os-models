package com.cdd.models.spark.pipeline.transformer

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.FilterSameFeatureValues
import org.apache.spark.ml.linalg.Vector
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/21/17.
  */
class FilterSameFeatureValuesSpec extends FunSpec with Matchers {

  /*
  describe("Running a classification model on CDK properties and filtering") {
    var builder = new CdkInputBuilder("data/continuous/Chagas.sdf.gz")
      .setActivityField("IC50_uM")
      .setActivityTransform(ActivityTransform.transformUmolIc50)
      .build(ModelContext.sparkSession)
      .addDescriptorsColumn()
    var df = builder.getDf()
    df = DataFrameOps.filterForEstimator(df, "cdk_descriptors")

    it("should have loaded 741 molecules") {
      df.count() should be(741)
    }

    val filterFeatures = (new FilterSameFeatureValues).setInputCol("rdkit")
    val filterModel = filterFeatures.fit(df)
    df = filterModel.transform(df)
    val count = df.count()

  }
  */

  describe("Running a classification model on RDKit properties and filtering") {

    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    //var df = ModelContext.sparkSession.read.parquet(Util.getProjectFilePath("data/discrete/data_chagas.parquet").getAbsolutePath)
    df = df.select("no", "activity_value", "rdkit_descriptors")
      .toDF("no", "label", "rdkit").filter("label is not null and rdkit is not null").coalesce(1).cache()

    it("should have loaded 741 molecules") {
      df.count() should be(741)
    }

    val filterFeatures = (new FilterSameFeatureValues).setInputCol("rdkit")
    val filterModel = filterFeatures.fit(df)
    df = filterModel.transform(df)
    val count = df.count()

    it("should have 115 input features") {
      df.head.getAs("rdkit").asInstanceOf[Vector].size should be(115)
    }
    it ("should have 106 unique features") {
      df.head.getAs("uniqueFeatures").asInstanceOf[Vector].size should be(106)
    }

  }
}
