package com.cdd.models.spark.pipeline.transformer

import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.transformer.SvdMlibTransformer
import org.apache.spark.ml.linalg.DenseVector
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 6/19/17.
  */
class SvdMlibTransformerSpec extends FunSpec with Matchers {

  describe("Performing SVD on sparse fingerprints") {

    var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
    // var df = ModelContext.sparkSession.read.parquet(Util.getProjectFilePath("data/discrete/data_chagas.parquet").getAbsolutePath)
    df = df.select("no", "activity_value", "fingerprints_RDKit_FCFP6")
      .toDF("no", "label", "rdkit").filter("label is not null and rdkit is not null").cache()
    val svdTransform = new SvdMlibTransformer().setInputCol("rdkit").setK(200)
    val svdModel = svdTransform.fit(df)
    df = svdModel.transform(df)
    val count = df.count()
    val data = df.collectAsList()
    info("Done the transform")

    it ("should have loaded 741 compounds") {
      df.count should be(741)
    }

    it ("should have added a svdFeatures column") {
      df.columns should contain("svdFeatures")
      df.select("svdFeatures").take(1)(0)(0).asInstanceOf[DenseVector].size should be(200)
    }
  }
}