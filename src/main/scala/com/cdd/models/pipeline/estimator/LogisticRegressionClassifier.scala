package com.cdd.models.pipeline.estimator

import java.util.Locale

import com.cdd.models.datatable.{DataTable, DenseVector, VectorBase}
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.transformer.StandardScaler

import scala.collection.mutable

// adapted from org.apache.spark.ml.classification.LogisticRegression

trait LogisticRegressionClassifierParams extends LinearRegressionParams {
  val threshold = new Parameter[Double](this, "threshold", "Threshold for binary classification")

  def setThreshold(value: Double): this.type = setParameter(threshold, value)

  setDefaultParameter(threshold, 0.5)

  val family = new Parameter[String](this, "family", "Method family: one of binomial, multinomial or auto")

  def setFamily(value:String): this.type = setParameter(family, value)

  setDefaultParameter(family, "auto")
}

class LogisticRegressionClassifier(val uid: String) extends PredictorWithProbabilities[LogisticRegressionClassifierModel]
  with LogisticRegressionClassifierParams with HasFeaturesAndLabelParameters {

  def this() = this(Identifiable.randomUID("logReg"))

  /**
    * Fits a model to the input data.
    */
  override def fit(dataTable: DataTable): LogisticRegressionClassifierModel = {
    val (labels, features) = extractFeaturesAndLabels(dataTable)
    val weights: Option[Vector[Double]] = None

    // spark parameters, that I haven't set as parameters:
    val fitIntercept = true
    val usingBoundConstrainedOptimization = false

    val labelSummarizer =
      labels.zipWithIndex.foldLeft(new MultiClassSummarizer) { case (summarizer, (label, index)) =>
        val weight = weights match {
          case Some(w) => w(index)
          case none => 1.0
        }
        summarizer.add(label, weight)
      }
    // would need to add weighted statistics if weights were used
    require(weights.isEmpty)
    val featuresStatistics = StandardScaler.summaryStatisticsArrayForFeatures(features)

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = featuresStatistics.size
    val numClasses = histogram.size
    // should always be two classes for activity data
    require(numClasses == 2)

    val isMultinomial = getParameter(family).toLowerCase(Locale.ROOT) match {
      case "binomial" =>
        require(numClasses == 1 || numClasses == 2, s"Binomial family only supports 1 or 2 " +
          s"outcome classes but found $numClasses.")
        false
      case "multinomial" => true
      case "auto" => numClasses > 2
      case other => throw new IllegalArgumentException(s"Unsupported family: $other")
    }
    val numCoefficientSets = if (isMultinomial) numClasses else 1

    val (coefficientMatrix, interceptVector, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
          s"Found $numInvalid invalid labels."
        logger.error(msg)
        throw new RuntimeException(msg)
      }

      val isConstantLabel = histogram.count(_ != 0.0) == 1

      (null, null, null)
      // TODO complete estimator
      /*
      if (fitIntercept && isConstantLabel && !usingBoundConstrainedOptimization) {
        logger.warn(s"All labels are the same value and fitIntercept=true, so the coefficients " +
          s"will be zeros. Training is not needed.")
        val constantLabelIndex = new DenseVector(histogram).argmax
        val coefMatrix = new SparseMatrix(numCoefficientSets, numFeatures,
          new Array[Int](numCoefficientSets + 1), Array.empty[Int], Array.empty[Double],
          isTransposed = true).compressed
        val interceptVec = if (isMultinomial) {
          Vectors.sparse(numClasses, Seq((constantLabelIndex, Double.PositiveInfinity)))
        } else {
          Vectors.dense(if (numClasses == 2) Double.PositiveInfinity else Double.NegativeInfinity)
        }
        (coefMatrix, interceptVec, Array.empty[Double])
      } else {
        if (!$(fitIntercept) && isConstantLabel) {
          logWarning(s"All labels belong to a single class and fitIntercept=false. It's a " +
            s"dangerous ground, so the algorithm may not converge.")
        }

        val featuresMean = summarizer.mean.toArray
        val featuresStd = summarizer.variance.toArray.map(math.sqrt)

        if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
          featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
          logWarning("Fitting LogisticRegressionModel without intercept on dataset with " +
            "constant nonzero column, Spark MLlib outputs zero coefficients for constant " +
            "nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.")
        }

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val bcFeaturesStd = instances.context.broadcast(featuresStd)
        val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept),
          $(standardization), bcFeaturesStd, regParamL2, multinomial = isMultinomial,
          $(aggregationDepth))

        val numCoeffsPlusIntercepts = numFeaturesPlusIntercept * numCoefficientSets

        val (lowerBounds, upperBounds): (Array[Double], Array[Double]) = {
          if (usingBoundConstrainedOptimization) {
            val lowerBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.NegativeInfinity)
            val upperBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.PositiveInfinity)
            val isSetLowerBoundsOnCoefficients = isSet(lowerBoundsOnCoefficients)
            val isSetUpperBoundsOnCoefficients = isSet(upperBoundsOnCoefficients)
            val isSetLowerBoundsOnIntercepts = isSet(lowerBoundsOnIntercepts)
            val isSetUpperBoundsOnIntercepts = isSet(upperBoundsOnIntercepts)

            var i = 0
            while (i < numCoeffsPlusIntercepts) {
              val coefficientSetIndex = i % numCoefficientSets
              val featureIndex = i / numCoefficientSets
              if (featureIndex < numFeatures) {
                if (isSetLowerBoundsOnCoefficients) {
                  lowerBounds(i) = $(lowerBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
                if (isSetUpperBoundsOnCoefficients) {
                  upperBounds(i) = $(upperBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
              } else {
                if (isSetLowerBoundsOnIntercepts) {
                  lowerBounds(i) = $(lowerBoundsOnIntercepts)(coefficientSetIndex)
                }
                if (isSetUpperBoundsOnIntercepts) {
                  upperBounds(i) = $(upperBoundsOnIntercepts)(coefficientSetIndex)
                }
              }
              i += 1
            }
            (lowerBounds, upperBounds)
          } else {
            (null, null)
          }
        }

        val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
          if (lowerBounds != null && upperBounds != null) {
            new BreezeLBFGSB(
              BDV[Double](lowerBounds), BDV[Double](upperBounds), $(maxIter), 10, $(tol))
          } else {
            new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
          }
        } else {
          val standardizationParam = $(standardization)
          def regParamL1Fun = (index: Int) => {
            // Remove the L1 penalization on the intercept
            val isIntercept = $(fitIntercept) && index >= numFeatures * numCoefficientSets
            if (isIntercept) {
              0.0
            } else {
              if (standardizationParam) {
                regParamL1
              } else {
                val featureIndex = index / numCoefficientSets
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                if (featuresStd(featureIndex) != 0.0) {
                  regParamL1 / featuresStd(featureIndex)
                } else {
                  0.0
                }
              }
            }
          }
          new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
        }

        /*
          The coefficients are laid out in column major order during training. Here we initialize
          a column major matrix of initial coefficients.
         */
        val initialCoefWithInterceptMatrix =
          Matrices.zeros(numCoefficientSets, numFeaturesPlusIntercept)

        val initialModelIsValid = optInitialModel match {
          case Some(_initialModel) =>
            val providedCoefs = _initialModel.coefficientMatrix
            val modelIsValid = (providedCoefs.numRows == numCoefficientSets) &&
              (providedCoefs.numCols == numFeatures) &&
              (_initialModel.interceptVector.size == numCoefficientSets) &&
              (_initialModel.getFitIntercept == $(fitIntercept))
            if (!modelIsValid) {
              logWarning(s"Initial coefficients will be ignored! Its dimensions " +
                s"(${providedCoefs.numRows}, ${providedCoefs.numCols}) did not match the " +
                s"expected size ($numCoefficientSets, $numFeatures)")
            }
            modelIsValid
          case None => false
        }

        if (initialModelIsValid) {
          val providedCoef = optInitialModel.get.coefficientMatrix
          providedCoef.foreachActive { (classIndex, featureIndex, value) =>
            // We need to scale the coefficients since they will be trained in the scaled space
            initialCoefWithInterceptMatrix.update(classIndex, featureIndex,
              value * featuresStd(featureIndex))
          }
          if ($(fitIntercept)) {
            optInitialModel.get.interceptVector.foreachActive { (classIndex, value) =>
              initialCoefWithInterceptMatrix.update(classIndex, numFeatures, value)
            }
          }
        } else if ($(fitIntercept) && isMultinomial) {
          /*
             For multinomial logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercepts such that
             it follows the distribution of the labels.
             {{{
               P(1) = \exp(b_1) / Z
               ...
               P(K) = \exp(b_K) / Z
               where Z = \sum_{k=1}^{K} \exp(b_k)
             }}}
             Since this doesn't have a unique solution, one of the solutions that satisfies the
             above equations is
             {{{
               \exp(b_k) = count_k * \exp(\lambda)
               b_k = \log(count_k) * \lambda
             }}}
             \lambda is a free parameter, so choose the phase \lambda such that the
             mean is centered. This yields
             {{{
               b_k = \log(count_k)
               b_k' = b_k - \mean(b_k)
             }}}
           */
          val rawIntercepts = histogram.map(c => math.log(c + 1)) // add 1 for smoothing
          val rawMean = rawIntercepts.sum / rawIntercepts.length
          rawIntercepts.indices.foreach { i =>
            initialCoefWithInterceptMatrix.update(i, numFeatures, rawIntercepts(i) - rawMean)
          }
        } else if ($(fitIntercept)) {
          /*
             For binary logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercept such that
             it follows the distribution of the labels.

             {{{
               P(0) = 1 / (1 + \exp(b)), and
               P(1) = \exp(b) / (1 + \exp(b))
             }}}, hence
             {{{
               b = \log{P(1) / P(0)} = \log{count_1 / count_0}
             }}}
           */
          initialCoefWithInterceptMatrix.update(0, numFeatures,
            math.log(histogram(1) / histogram(0)))
        }

        if (usingBoundConstrainedOptimization) {
          // Make sure all initial values locate in the corresponding bound.
          var i = 0
          while (i < numCoeffsPlusIntercepts) {
            val coefficientSetIndex = i % numCoefficientSets
            val featureIndex = i / numCoefficientSets
            if (initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) < lowerBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, lowerBounds(i))
            } else if (
              initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) > upperBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, upperBounds(i))
            }
            i += 1
          }
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          new BDV[Double](initialCoefWithInterceptMatrix.toArray))

        /*
           Note that in Logistic Regression, the objective history (loss + regularization)
           is log-likelihood which is invariant under feature standardization. As a result,
           the objective history from optimizer is the same as the one in the original space.
         */
        val arrayBuilder = mutable.ArrayBuilder.make[Double]
        var state: optimizer.State = null
        while (states.hasNext) {
          state = states.next()
          arrayBuilder += state.adjustedValue
        }
        bcFeaturesStd.destroy(blocking = false)

        if (state == null) {
          val msg = s"${optimizer.getClass.getName} failed."
          logError(msg)
          throw new SparkException(msg)
        }

        /*
           The coefficients are trained in the scaled space; we're converting them back to
           the original space.

           Additionally, since the coefficients were laid out in column major order during training
           to avoid extra computation, we convert them back to row major before passing them to the
           model.

           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val allCoefficients = state.x.toArray.clone()
        val allCoefMatrix = new DenseMatrix(numCoefficientSets, numFeaturesPlusIntercept,
          allCoefficients)
        val denseCoefficientMatrix = new DenseMatrix(numCoefficientSets, numFeatures,
          new Array[Double](numCoefficientSets * numFeatures), isTransposed = true)
        val interceptVec = if ($(fitIntercept) || !isMultinomial) {
          Vectors.zeros(numCoefficientSets)
        } else {
          Vectors.sparse(numCoefficientSets, Seq())
        }
        // separate intercepts and coefficients from the combined matrix
        allCoefMatrix.foreachActive { (classIndex, featureIndex, value) =>
          val isIntercept = $(fitIntercept) && (featureIndex == numFeatures)
          if (!isIntercept && featuresStd(featureIndex) != 0.0) {
            denseCoefficientMatrix.update(classIndex, featureIndex,
              value / featuresStd(featureIndex))
          }
          if (isIntercept) interceptVec.toArray(classIndex) = value
        }

        if ($(regParam) == 0.0 && isMultinomial && !usingBoundConstrainedOptimization) {
          /*
            When no regularization is applied, the multinomial coefficients lack identifiability
            because we do not use a pivot class. We can add any constant value to the coefficients
            and get the same likelihood. So here, we choose the mean centered coefficients for
            reproducibility. This method follows the approach in glmnet, described here:

            Friedman, et al. "Regularization Paths for Generalized Linear Models via
              Coordinate Descent," https://core.ac.uk/download/files/153/6287975.pdf
           */
          val centers = Array.fill(numFeatures)(0.0)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            centers(j) += v
          }
          centers.transform(_ / numCoefficientSets)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            denseCoefficientMatrix.update(i, j, v - centers(j))
          }
        }

        // center the intercepts when using multinomial algorithm
        if ($(fitIntercept) && isMultinomial && !usingBoundConstrainedOptimization) {
          val interceptArray = interceptVec.toArray
          val interceptMean = interceptArray.sum / interceptArray.length
          (0 until interceptVec.size).foreach { i => interceptArray(i) -= interceptMean }
        }
        (denseCoefficientMatrix.compressed, interceptVec.compressed, arrayBuilder.result())
      }
      */
    }

    null
  }
}

class LogisticRegressionClassifierModel(override val uid: String)
  extends PredictionModelWithProbabilities[LogisticRegressionClassifierModel]
    with LogisticRegressionClassifierParams {

  override def transformRow(features: VectorBase): (Double, Double) = {
    return (.0, .0)
  }
}

private class MultiClassSummarizer {

  // The first element of value in distinctMap is the actually number of instances,
  // and the second element of value is sum of the weights.
  private val distinctMap = new mutable.HashMap[Int, (Long, Double)]
  private var totalInvalidCnt: Long = 0L

  /**
    * Add a new label into this MultilabelSummarizer, and update the distinct map.
    *
    * @param label  The label for this data point.
    * @param weight The weight of this instances.
    * @return This MultilabelSummarizer
    */
  def add(label: Double, weight: Double = 1.0): this.type = {
    require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

    if (weight == 0.0) return this

    if (label - label.toInt != 0.0 || label < 0) {
      totalInvalidCnt += 1
      this
    }
    else {
      val (counts: Long, weightSum: Double) = distinctMap.getOrElse(label.toInt, (0L, 0.0))
      distinctMap.put(label.toInt, (counts + 1L, weightSum + weight))
      this
    }
  }

  /** @return The total invalid input counts. */
  def countInvalid: Long = totalInvalidCnt

  /** @return The number of distinct labels in the input dataset. */
  def numClasses: Int = if (distinctMap.isEmpty) 0 else distinctMap.keySet.max + 1

  /** @return The weightSum of each label in the input dataset. */
  def histogram: Vector[Double] = {
    val result = Array.ofDim[Double](numClasses)
    var i = 0
    val len = result.length
    while (i < len) {
      result(i) = distinctMap.getOrElse(i, (0L, 0.0))._2
      i += 1
    }
    result.toVector
  }


}