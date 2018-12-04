package com.cdd.spark.pipeline.tuning

import com.cdd.models.utils.HasLogging
import com.github.fommil.netlib.F2jBLAS
import org.apache.log4j.Logger
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Dataset

/**
  * Hyperparameter optimization metrics
  *
  * @param bestIndex
  * @param avgMetrics
  * @param splitMetrics
  * @param estimatorParamMaps
  */
case class CrossValidationMetrics(bestIndex:Int, avgMetrics:Array[Double], splitMetrics:Array[Array[Double]], estimatorParamMaps: Array[ParamMap])

/**
  * Extends cross validator to optimized parameters in a memory friendly way (i.e. by not creating an array of models).
  * SVM models take up a lot of memory, so memory errors are encountered when using the regular fit method.
  *
  * It would make more sense to override the @see CrossValidator#fit() method, but that returns a CrossValidatorModel
  * which has no public constructor.
  *
  * Unlike cross validator we also store the individual fold metrics in addition to average metrics
  *
  * Created by gjones on 7/11/17.
  */
class MemoryOptimizedCrossValidator(override val uid: String) extends CrossValidator with HasLogging{

  def this() = this(Identifiable.randomUID("memopt_cv"))

  private val f2jBLAS = new F2jBLAS

  /**
    * Perform hyperparameter search using cross validation
    *
    * @param dataset
    * @return
    */
  def optimizeParameters(dataset: Dataset[_]):  CrossValidationMetrics = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val nModels = epm.length
    val nFolds = $(numFolds)
    val metrics = new Array[Double](nModels)
    val splitMetrics = Array.ofDim[Double](nModels, nFolds)
    val splits = MLUtils.kFold(dataset.toDF.rdd, nFolds, $(seed))

    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()

      epm.zipWithIndex.foreach { case (pm, pmIndex) =>
        var model = est.fit(trainingDataset, pm).asInstanceOf[Model[_]]
        val metric = eval.evaluate(model.transform(validationDataset, pm))
        logger.warn(s"Got metric $metric for model trained with ${epm(pmIndex)} fold ${splitIndex+1}.")
        splitMetrics(pmIndex)(splitIndex) = metric
        metrics(pmIndex) += metric
        logger.warn(s"Memory ${Runtime.getRuntime().totalMemory()/(1024*1024)}M total ${Runtime.getRuntime().freeMemory()/(1024*1024)}M free")
        // For whatever reason I need to set model to null here otherwise it will not be GCed- the problem is intermittent- but I've observed it using jvisualvm
        model = null
        System.gc()
      }

      trainingDataset.unpersist()
      validationDataset.unpersist()
    }

    f2jBLAS.dscal(nModels, 1.0 / nFolds, metrics, 1)
    logger.warn(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logger.warn(s"Best set of parameters:\n${epm(bestIndex)}")
    logger.warn(s"Best cross-validation metric: $bestMetric.")

    CrossValidationMetrics(bestIndex, metrics, splitMetrics, epm)
  }
}

