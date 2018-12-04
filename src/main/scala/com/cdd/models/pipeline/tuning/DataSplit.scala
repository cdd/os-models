package com.cdd.models.pipeline.tuning

import com.cdd.models.datatable.DataTable
import com.cdd.models.pipeline._
import com.cdd.models.utils.HasLogging
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

class IdenticalSplitLabels(val msg:String) extends Exception(msg)

@SerialVersionUID(1000L)
class DataSplit(override val uid: String) extends HasParameters with HasLogging with Serializable {

  def this() = this(Identifiable.randomUID("dataSplit"))

  private var predictDt: Option[DataTable] = None
  private var metric: Option[Double] = None
  private val trainingMetrics = ArrayBuffer[Double]()
  private val testMetrics = ArrayBuffer[Double]()

  val estimator: Parameter[Estimator[_, _]] = new Parameter[Estimator[_, _]](this, "estimator", "estimator used in CV")

  def setEstimator(value: Estimator[_, _]): this.type = setParameter(estimator, value)

  val evaluator: Parameter[Evaluator] = new Parameter(this, "evaluator",
    "evaluator used")

  def setEvaluator(value: Evaluator): this.type = setParameter(evaluator, value)

  val numFolds: Parameter[Int] = new Parameter[Int](this, "numFolds",
    "number of folds (>= 2)")

  setDefaultParameter(numFolds, 5)

  def setNumFolds(value: Int): this.type = setParameter(numFolds, value)

  def foldAndFit(inputDt: DataTable): Unit = {
    val nFolds = getParameter(numFolds)
    trainingMetrics.clear()
    testMetrics.clear()
    val pred = getParameter(estimator)
    val eval = getParameter(evaluator)

    val predictDts = (0 until nFolds).map { foldNo =>
      val (trainingDt, testDt) = inputDt.trainValidateFold(foldNo, nFolds)
      pred match {
        case est: HasLabelColumn =>
          val uniqueValuesCount = trainingDt.column(est.getParameter(est.labelColumn)).uniqueValues().size
          if (uniqueValuesCount < 2)
            throw new IdenticalSplitLabels("Unable to process - all values for split identical")
        case _ =>
      }
      val model = pred.fit(trainingDt).asInstanceOf[RowTransformer[Any, Any]]
      val testPredict = model.transform(testDt)
      val testMetric = eval.evaluate(testPredict)
      testMetrics += testMetric
      val trainingPredict = model.transform(trainingDt)
      val trainingMetric = eval.evaluate(trainingPredict)
      trainingMetrics += trainingMetric
      logger.info(s"Datasplit fold ${foldNo + 1} test metric $testMetric train metric $trainingMetric")
      testPredict.addConstantColumn("foldNo", foldNo)
    }

    val dt = predictDts.reduceLeft { (a, b) => a.append(b) }
    assert(dt.length == inputDt.length)
    metric = Some(eval.evaluate(dt))
    logger.info(s"Overall prediction metric ${metric.get}")
    predictDt = Some(dt)
  }

  def getPredictDt(): DataTable = predictDt.get

  def getMetric(): Double = metric.get

  def getTrainingMetrics(): Vector[Double] = trainingMetrics.toVector

  def getTestMetrics(): Vector[Double] = testMetrics.toVector


}
