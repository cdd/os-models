package com.cdd.models.pipeline

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.cdd.models.datatable.{DataTable, ParallelDataTable, VectorBase}
import com.cdd.models.utils.{HasLogging, Util}
import org.apache.log4j.Logger

@SerialVersionUID(7246356460958221534L)
class Pipeline(override val uid: String) extends Estimator[PipelineModel, Any] with HasLabelColumn with HasLogging {

  def this() = this(Identifiable.randomUID("pipeline_"))

  val stages = new Parameter[Vector[PipelineStage]](this, "stages", "Pipeline stages")
  setDefaultParameter(stages, Vector.empty)

  def setStages(value: Vector[PipelineStage]): this.type = setParameter(stages, value)

  def appendStage(stage: PipelineStage): this.type  = {
    setStages(getParameter(stages) :+ stage)
    this
  }

  override def copy(extra: ParameterMap): this.type = {
    val that = new Pipeline(uid)
    copyParameterValues(that)
    val newStagesArray = getParameter(stages).map { s => s.copy(extra) }
    that.setStages(newStagesArray)
    that.asInstanceOf[this.type]
  }

  override def fit(dataTable: DataTable): PipelineModel = {
    logger.debug(s"Performing fitting on Pipeline with parameters\n${parameterInfo()}")
    var dt = dataTable
    val transformers = getParameter(stages).map { stage =>
      val transformer = stage match {
        case est: EstimatorBase[_, _, _] =>
          est.fit(dt)
        case t: Transformer => t
      }
      //TODO don't have to do any transforms after the last estimator in the pipeline (see /org/apache/spark/ml/Pipeline.scala)
      dt = transformer.transform(dt)
      transformer
    }
    copyParameterValues(new PipelineModel(uid, transformers))
  }

  override def parameterInfo(): String = {
    getParameter(stages).map { s => s"Stage ${s.uid} \n ${s.parameterInfo()}" }.mkString("\n")
  }

  override def setLabelColumn(value: String): Pipeline.this.type = {
    val stages = getParameter(this.stages)
    require(stages.nonEmpty)

    stages.find(_.isInstanceOf[HasLabelColumn]) match {
      case Some(hasLabelStage:HasLabelColumn) => hasLabelStage.setLabelColumn(value)
      case _ => throw new IllegalArgumentException("Unable to find has label column stage in pipeline")
    }

    this
  }
}

object PipelineModel {
  def loadPipeline(objectFile: String): PipelineModel = {
    Util.deserializeObject[PipelineModel](objectFile)
  }
}

@SerialVersionUID(-5416118145878623441L)
class PipelineModel(override val uid: String, val pipelineModels: Vector[Transformer])
  extends Model[PipelineModel, Any] with HasFeaturesParameters {

  def this(pipelineModels:Vector[Transformer]) =
    this(Identifiable.randomUID("pipeline_"), pipelineModels)

  override def transform(dataTable: DataTable): DataTable = {
    pipelineModels.foldLeft(dataTable) { case (dt, transformer) => transformer.transform(dt) }
  }

  override def transformRow(features: VectorBase): Any = {
    throw new NotImplementedError()
  }

  def savePipeline(objectFile: String): Unit = {
    Util.serializeObject(objectFile, this)
  }

  override def getFeaturesColumn(): String = {
    pipelineModels.find(_.isInstanceOf[HasFeaturesParameters]) match {
      case Some(hasFeaturesModel:HasFeaturesParameters) => hasFeaturesModel.getFeaturesColumn()
      case _ => throw new IllegalArgumentException("Unable to find has feature column stage in pipeline model")
    }
  }

  def getEstimatorId(): String = {
    val opt = pipelineModels.find { p =>
      p.isInstanceOf[PredictionModel[_]] || p.isInstanceOf[PredictionModelWithProbabilities[_]]
    }
    val consOpt = pipelineModels.find(_.isInstanceOf[ConsensusProbabilityClassificationModel])
    require(opt.isDefined)
    consOpt match {
      case Some(consModel) => consModel.asInstanceOf[ConsensusProbabilityClassificationModel].modelIds().mkString("|")
      case _ =>
        opt.get.uid
    }
  }
}
