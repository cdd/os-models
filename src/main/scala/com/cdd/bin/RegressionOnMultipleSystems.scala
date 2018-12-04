package com.cdd.bin

import com.cdd.models.datatable.{ActivityTransform, DataTable, SdfileReader}
import com.cdd.models.pipeline.estimator.SvmRegressor
import com.cdd.models.pipeline.transformer.FilterSameFeatureValues
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.pipeline.{Pipeline, RegressionEvaluator}
import com.cdd.models.utils.{ActivityCategory, SdfileActivityField}
import org.knowm.xchart.{BitmapEncoder, XYChartBuilder}
import org.knowm.xchart.BitmapEncoder.BitmapFormat
import org.knowm.xchart.internal.chartpart.Chart

import scala.collection.JavaConverters._

object RegressionOnMultipleSystems extends App {

  if (args.length < 3) {
    print(s"Usage: ${getClass} <sd_field_name> <transform_name> <outputfile> <sdfiles>")
  }
  else {
    val fieldName = args.head
    val transform = ActivityTransform.stringToTransform(args(1))
    val property = SdfileActivityField(fieldName, ActivityCategory.CONTINUOUS, transform)
    val outputFile = args(2)
    val (_, files) = args.splitAt(3)
    val results = files.foldLeft[Option[DataTable]](None) { (results, sdfile) =>
      var dt = SdfileReader.sdFileToRdkitDataTable(sdfile, Vector(property))
      dt = dt.selectAs(("no" -> "no"), ("activity_value" -> "label"), ("fingerprints_RDKit_FCFP6" -> "rdkit_fp")).filterNull().shuffle()
      val filter = new FilterSameFeatureValues().setFeaturesColumn("rdkit_fp").setOutputColumn("uniqueFeatures")
      val svr = new SvmRegressor().setKernelType("TANIMOTO").setGamma(1.0).setFeaturesColumn("uniqueFeatures").setC(1.0).setCoef0(2.0).setDegree(2)
      val pipeline = new Pipeline().setStages(Vector(filter, svr))
      val evaluator = new RegressionEvaluator()
      val dataSplit = new DataSplit().setNumFolds(5).setEvaluator(evaluator).setEstimator(pipeline)
      dataSplit.foldAndFit(dt)
      println(s"Test system $sdfile RMSE ${dataSplit.getMetric()}")
      val systemResults = dataSplit.getPredictDt().select("label", "prediction").addConstantColumn[String]("System", sdfile)
      results match {
        case None => Some(systemResults)
        case Some(r) => Some(r.append(systemResults))
      }
    }
    results.get.exportToFile(outputFile)
  }
}

object ProcessRegressionOnMultipleSystems extends App {
  if (args.length != 2 && args.length != 3) {
    print(s"Usage: ${getClass} <results_file> <image_file> [n_cols]")
  }
  else {
    val dt = DataTable.loadFile(args.head)
    val testSystems = dt.column("System").uniqueValues().asInstanceOf[Vector[String]]
    var (dataTables, titles) = testSystems.map { t =>
      val tdt = dt.filterByColumnValues("System", Vector(t))
      val title = t.take(30)
      (tdt, title)
    }.unzip

    val cols = if (args.length == 3) {
      args(2).toInt
    } else {
      3
    }
    new RegressionEvaluator().gridPlot(args(1), dataTables, titles, cols=cols)

  }
}