package com.cdd.models.spark.pipeline

import breeze.numerics.log10
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.{Configuration, Util}
import com.cdd.spark.pipeline.CdkInputBuilder
import com.cdd.spark.pipeline.transformer.VectorFolder
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.sql.functions
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gareth on 5/17/17.
  */
class SimpleModelSpec extends FunSpec with Matchers {


  val sparkSession = Configuration.sparkSession
  var df = Configuration.sparkSession.read.parquet(Util.getProjectFilePath("data/continuous/Chagas.parquet").getAbsolutePath)
  df = DataFrameOps.filterForEstimator(df, "fingerprints_CDK_ECFP6")
  val folder = new VectorFolder().setFoldSize(200)
  df = folder.transform(df)

  describe("Running a model on a dataframe") {

    it("should have created a dataframe of 743 rows") {
      df.count() should be(743)
    }

    val lr = new GeneralizedLinearRegression().setMaxIter(10).setRegParam(0.3).setLink("log").setFamily("gaussian").setFeaturesCol(folder.get(folder.outputCol).get);
    df.cache()
    val lrModel = lr.fit(df)
    info(s"Model summary ${lrModel.summary}")

    it("should have created a model with 674 residuals") {
      lrModel.summary.residuals().count() should be(743)
    }

    val predictDf = lrModel.transform(df)
    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDf)
    it ("should have a model rmse of about 3.1") {
      rmse should be (3.2 +- 0.1)
    }
    info(s"rmse is $rmse")



  }

  describe("Running a model on a dataframe split into test and training") {

    val pIc50Transform = (ic50:Double) => -log10(ic50*1e-6)
    val func = functions.udf(pIc50Transform)
    var df2 = df.withColumnRenamed("label", "Ic50")
    val df3 = df2.withColumn("label", func(functions.col("Ic50")))

    val lr = new GeneralizedLinearRegression().setMaxIter(10).setRegParam(0.3).setLink("log").setFamily("gaussian")

    val Array(training, test) = df3.randomSplit(Array(0.8, 0.2))

    val lrModel = lr.fit(training)
    info(s"Model summary ${lrModel.summary}")
    //val trainValidationSplit = new TrainValidationSplit().setEstimator(lr).setEvaluator(evaluator).setTrainRatio(.8)

    val predictDf = lrModel.transform(test)
    val evaluator = new RegressionEvaluator()
    val rmse = evaluator.evaluate(predictDf)

    info(s"rmse is $rmse")

    val c = predictDf.stat.corr("label", "prediction")
    info(s"correlation coefficient $c")


    info("done split")

    val rmse2 = DataFrameOps.rmse(predictDf, "label", "prediction")
    info(s"rmse2 is $rmse2")

  }

}
