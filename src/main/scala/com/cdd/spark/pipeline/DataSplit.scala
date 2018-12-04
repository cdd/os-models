package com.cdd.spark.pipeline

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by gjones on 5/22/17.
  */
class DataSplit(override val uid: String) extends Params {
  private var predictDf: Option[DataFrame] = None
  private var metric: Option[Double] = None
  private var trainingMetrics: Option[Array[Double]] = None
  private var testMetrics: Option[Array[Double]] = None
  var probabilityColumn: String = "probability"


  final val seed: LongParam = new LongParam(this, "seed", "random seed")
  setDefault(seed, this.getClass.getName.hashCode.toLong)

  final def getSeed: Long = $(seed)

  def setSeed(value: Long): this.type = set(seed, value)

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")

  def getEstimator: Estimator[_] = $(estimator)

  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")

  def getEvaluator: Evaluator = $(evaluator)

  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds (>= 2)")

  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 5)

  def setNumFolds(value: Int): this.type = set(numFolds, value)

  def foldAndFit(inputDf: DataFrame): Unit = {
    val sparkSession = inputDf.sparkSession
    val splits = MLUtils.kFold(inputDf.rdd, $(numFolds), $(seed))
    trainingMetrics = Some(new Array[Double](getNumFolds))
    testMetrics = Some(new Array[Double](getNumFolds))
    val predictDfs = splits.zipWithIndex.map { case ((trainingPartition, testPartition), index) =>
      val trainingDf = sparkSession.createDataFrame(trainingPartition, inputDf.schema).cache()
      val testDf = sparkSession.createDataFrame(testPartition, inputDf.schema).cache()
      val model = getEstimator.fit(trainingDf).asInstanceOf[Transformer]
      val testPredict = model.transform(testDf)
      testMetrics.get(index) = $(evaluator).evaluate(testPredict)
      val trainingPredict = model.transform(trainingDf)
      trainingMetrics.get(index) = $(evaluator).evaluate(trainingPredict)

      testDf.unpersist()
      trainingDf.unpersist()
      testPredict
    }
    val df = predictDfs.reduce((df, current) => df.union(current)).cache()
    predictDf = Some(df)
    metric = Some($(evaluator).evaluate(predictDf.get))
  }

  def getPredictDf(): DataFrame = predictDf.get

  def getMetric(): Double = metric.get

  def getTrainingMetrics() : Array[Double] = trainingMetrics.get

  def getTestMetrics() : Array[Double] = testMetrics.get


  def getCorrelation(): Double = {
    predictDf.get.stat.corr("label", "prediction")
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("DataSplit"))
}

class ClassifierDataSplit(override val uid: String) extends DataSplit {
  def this() = this(Identifiable.randomUID("DataSplit"))

  private var binaryClassificationMetrics: Option[BinaryClassificationMetrics] = None

  def rocDf(): DataFrame = {
    val predictDf = getPredictDf()
    val probDf = predictDf.select(probabilityColumn, "label")
    val probInfo = probDf.rdd.map { r => (r.getAs[Vector](0)(1), r.getDouble(1)) }
    val metrics = new BinaryClassificationMetrics(probInfo)
    this.binaryClassificationMetrics = Some(metrics)
    val rocRdd = metrics.roc();
    predictDf.sparkSession.createDataFrame(rocRdd).toDF("FPR", "TPR")
  }

  def getBinaryClassificationMetrics(): BinaryClassificationMetrics = binaryClassificationMetrics.get
}