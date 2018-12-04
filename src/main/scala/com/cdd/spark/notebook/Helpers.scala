package com.cdd.spark.notebook

import com.cdd.models.datatable.ActivityTransform
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.FingerprintTransform
import com.cdd.models.utils.Configuration
import com.cdd.spark.pipeline.CdkInputBuilder
import com.cdd.spark.utils.DataFrameOps
import org.apache.spark.sql.DataFrame

/**
  * Created by gjones on 5/30/17.
  */
object Helpers {

  def exampleChagasCdkContinuousDf(sparse: Boolean = false): DataFrame = {
    val fingerprintTransform = if (sparse) FingerprintTransform.Sparse else FingerprintTransform.Fold
    val builder = new CdkInputBuilder("data/continuous/Chagas.sdf.gz")
      .setActivityField("IC50_uM")
      .setActivityTransform(ActivityTransform.transformUmolIc50)
    builder.build(Configuration.sparkSession)
      .addFingerprintColumn(CdkFingerprintClass.FCFP6, fingerprintTransform, 200)
    var df = builder.getDf()
    DataFrameOps.filterForEstimator(df,  builder.firstFingerprintColumn())
  }

  def exampleChagasCdkDiscreteDf(): DataFrame = {
    val builder = new CdkInputBuilder("data/discrete/data_chagas.sdf.gz")
      .setActivityField("Activity")
      .build(Configuration.sparkSession)
      .addFingerprintColumn(CdkFingerprintClass.ECFP6, FingerprintTransform.Fold, 200)
    var df = builder.getDf()
    DataFrameOps.filterForEstimator(df, builder.firstFingerprintColumn())
  }

}
