package com.cdd.models.pipeline

import java.io.FileFilter

import com.cdd.models.datatable.DataTable
import com.cdd.models.utils.Util
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.scalatest.{FunSpec, Matchers}

// need to have generated model files using SerializedPipelineSpec1, prior to running this spec
class SerializedPipelineSpec2 extends FunSpec with Matchers {

  describe("Making predictions using saved data") {
    val modelDirectory = Util.getProjectFilePath("data/saved_models")
    val fileFilter = new WildcardFileFilter("*_spec.model").asInstanceOf[FileFilter]
    modelDirectory.listFiles(fileFilter).map(_.getAbsolutePath).foreach { modelFileName =>

      describe(s"checking prediction for $modelFileName") {
        val model = PipelineModel.loadPipeline(modelFileName)
        val dataTable = DataTable.loadFile(modelFileName.replace(".model", ".csv.gz"))
          .rename("prediction" -> "previous_prediction")

        val predictDt = model.transform(dataTable)

        it (s"should have the same predictions as previously for $modelFileName") {
          predictDt.column("prediction").toDoubles()
            .zip(dataTable.column("previous_prediction").toDoubles())
            .foreach { case (p, pp) =>
              p should be (pp +- 0.000001)
            }
        }
      }


    }
  }
}