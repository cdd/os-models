package com.cdd.models.validation

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline.tuning.HyperparameterOptimizer
import com.cdd.models.utils.{HasLogging, LocalTestSystems}
import com.cdd.models.utils.TestSystems.TestSystemDir


object Tuner extends HasLogging{

  private[validation] def runTuning(resultsFileName: String, methodName: String, optimizer: HyperparameterOptimizer,
                                    mapper: (DataTable) => DataTable, includeNames: List[String] = List(),
                                    testSystemDir: TestSystemDir.Value = TestSystemDir.CONTINUOUS): Unit = {

    val (resultsFile, currentResults) = LocalTestSystems.prepareResultFile(resultsFileName)

    val func = (name: String, dt: DataTable) => {
      val dataTable = mapper(dt).filterNull().shuffle()
      logger.warn(s"Running CV on ${name}")
      logger.warn(s"Loaded datatable for ${name}")
      val results = optimizer.optimizeParameters(dataTable)
      results.logToFile(resultsFile, name, methodName)
    }

    val testSystems = LocalTestSystems.testSystemsFactory(testSystemDir, func)
    testSystems.applyFunctionToTestSystemsSequentially(includeNames = includeNames, excludeNames = currentResults.toList)

    logger.info("Done!")
  }
}
