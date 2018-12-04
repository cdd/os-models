package com.cdd.models.tox

import com.cdd.models.datatable.DataTable
import com.cdd.models.datatable.DataTableColumn.AllItemsNoneException
import com.cdd.models.pipeline.{PipelineModel, TestSmiles}
import com.cdd.models.tox.Tox21ConsensusModel.ToxResultSetType
import com.cdd.models.tox.Tox21TesterModel.Tox21TesterModel
import com.cdd.models.tox.tuning.ParameterizedModelApp
import com.cdd.models.utils.HasLogging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object Tox21TesterModel extends Enumeration {
  type Tox21TesterModel = Value
  val Best, Consensus, TunedBest, TunedConsensus, ConsensusIncludingScoring, TunedConsensusIncludingScoring = Value
}

object Tox21Tester extends HasLogging {

  class Tox21TesterResults(val predictTable: DataTable, val nPredicted: Int, val hitRate: Double, val model: PipelineModel, val compoundNos: Vector[Int])

  private val modelCache: mutable.Map[(String, Tox21TesterModel), PipelineModel] = mutable.Map()

  def model(toxField: String, testerModel: Tox21TesterModel): PipelineModel = this.synchronized {

    val modelPicker = () => {
      testerModel match {
        case Tox21TesterModel.Best => Tox21ClassificationModel.bestModel(toxField)
        case Tox21TesterModel.Consensus =>
          val toxResultsSet = Tox21ConsensusModel.toxResultSetFromType(ToxResultSetType.Best)
          Tox21ConsensusModel.model(toxResultsSet, toxField)
        case Tox21TesterModel.TunedBest =>
          ParameterizedModelApp.bestModel(toxField)
        case Tox21TesterModel.TunedConsensus =>
          val toxResultsSet = Tox21ConsensusModel.toxResultSetFromType(ToxResultSetType.Tuned)
          Tox21ConsensusModel.model(toxResultsSet, toxField)
        case Tox21TesterModel.ConsensusIncludingScoring =>
          val toxResultsSet = Tox21ConsensusModel.toxResultSetFromType(ToxResultSetType.BestIncludingScoring)
          Tox21ConsensusModel.model(toxResultsSet, toxField)
        case Tox21TesterModel.TunedConsensusIncludingScoring =>
          val toxResultsSet = Tox21ConsensusModel.toxResultSetFromType(ToxResultSetType.TunedIncludingScoring)
          Tox21ConsensusModel.model(toxResultsSet, toxField)
        case _ => throw new IllegalArgumentException
      }
    }

    modelCache.getOrElseUpdate((toxField, testerModel), modelPicker())
  }

  def testDataTable(toxField: String, tox21TesterModel: Tox21TesterModel, dataTable: DataTable, parallel: Boolean = false)
  : Tox21TesterResults = {
    logger.info(s"Test data table: data table size ${dataTable.length}")
    val model = this.model(toxField, tox21TesterModel)
    var inputDataTable =
      try {
        val featuresColumn = model.getFeaturesColumn()
        logger.info(s"Model for tox field $toxField uses feature column $featuresColumn")
        dataTable.select("no", "rdkit_smiles", "cdk_smiles", featuresColumn).filterNull()
      } catch {
        // consensus model
        case ex: IllegalArgumentException =>
          logger.info("Using all features columns")
          dataTable.filterNull()
      }

    logger.info(s"Test data table: filtered data table size ${inputDataTable.length}")
    val predictTable =
      if (parallel)
        model.parallelTransform(inputDataTable, 1000)
      else
        model.transform(inputDataTable)
    val compoundNos = predictTable.column("prediction").toDoubles().zip(predictTable.column("no").toInts())
      .filter(_._1 == 1.0)
      .map(_._2)
    val nPredicted = compoundNos.length
    val hitRate = nPredicted.toDouble / inputDataTable.length.toDouble
    logger.info(s"N tested ${predictTable.length} N predicted $nPredicted hit rate $hitRate")
    new Tox21TesterResults(predictTable, nPredicted, hitRate, model, compoundNos)
  }

  class CompoundActivitySummary(val compoundNo: Int, val smiles: String, val active: Array[String], val inactive: Array[String], val missing: Array[String]) {
    override def toString: String = {
      if (active.nonEmpty | missing.nonEmpty) {
        val missingStr = if (missing.nonEmpty) s" missing: ${missing.mkString(", ")}" else ""
        val activeStr = if (missing.nonEmpty) s" active: ${active.mkString(", ")}" else ""
        s"$compoundNo: $smiles:$activeStr$missingStr"
      } else {
        s"$compoundNo: $smiles: no tox activity"
      }
    }
  }

  class CompoundActivityHitrateResult(val hitRate: Double, val nHits: Int, val nTotal: Int, val compounds: Array[Int])

  def compoundActivityHitrate(toxField: String, results: Array[CompoundActivitySummary]): CompoundActivityHitrateResult = {
    val (nTotal, nHits, compounds) = results.foldRight((0, 0, List[Int]())) { case (result, (nT, nH, hits)) =>
      val missing = result.missing.contains(toxField)
      val hit = result.active.contains(toxField)
      val inactive = result.inactive.contains(toxField)
      if (missing) {
        assert(!hit)
        assert(!inactive)
        (nT, nH, hits)
      }
      else if (hit) {
        assert(!missing)
        assert(!inactive)
        (nT + 1, nH + 1, result.compoundNo :: hits)
      } else {
        assert(!hit)
        assert(!missing)
        assert(inactive)
        (nT + 1, nH, hits)
      }
    }
    new CompoundActivityHitrateResult(nHits.toDouble / nTotal.toDouble, nHits, nTotal, compounds.toArray)
  }

  def testSmilesAgainstMultiple(tox21TesterModel: Tox21TesterModel, smiArray: Array[String],
                                targets: Array[String] = Array(), parallel: Boolean = true): Array[CompoundActivitySummary] = {
    val skipRdkitDescriptors = tox21TesterModel match {
      case Tox21TesterModel.TunedBest | Tox21TesterModel.TunedConsensus
           | Tox21TesterModel.TunedConsensusIncludingScoring => false
      case _ => true
    }

    var dataTable = Try(TestSmiles.allDataTable(smiArray.toVector, skipRdKitDescriptors = skipRdkitDescriptors, parallel = parallel)) match {
      case Success(dt) => dt
      case Failure(_: AllItemsNoneException) => return Array.empty
      case Failure(ex) => throw ex
    }
    // there are issues in test reproducibility if the table row order is not restored
    if (parallel)
      dataTable = dataTable.sort("no")
    require(smiArray.length == dataTable.length)

    val toxFields = if (targets.isEmpty) Tox21ClassificationModel.toxAssays else targets.toVector
    val allResults = toxFields.map { toxField =>
      val results = testDataTable(toxField, tox21TesterModel, dataTable, parallel = parallel)

      val moleculeIndices = results.predictTable.column("no").toInts().map(_ - 1)
      val activities = results.predictTable.column("prediction").toDoubles().map {
        case 0.0 => false
        case 1.0 => true
        case _ => throw new IllegalArgumentException
      }

      val activeMap = moleculeIndices.zip(activities).map { case (index, active) => (index, active) }.toMap
      (toxField, activeMap)
    }

    smiArray.zipWithIndex.map { case (smiles, moleculeIndex) =>
      val activeTargets = ArrayBuffer[String]()
      val inactiveTargets = ArrayBuffer[String]()
      val missingTargets = ArrayBuffer[String]()

      allResults.foreach { case (toxField, activityMap) =>
        activityMap.get(moleculeIndex) match {
          case Some(true) => activeTargets.append(toxField)
          case Some(false) => inactiveTargets.append(toxField)
          case None => missingTargets.append(toxField)
        }
      }

      new CompoundActivitySummary(moleculeIndex + 1, smiles, activeTargets.toArray, inactiveTargets.toArray,
        missingTargets.toArray)
    }

  }

}
