package com.cdd.models.pipeline.estimator

// adapted from org.apache.spark.ml.regression.LinearRegression


import breeze.linalg.{DenseVector => BreezeDenseVector, SparseVector => BreezeSparseVector}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS, OWLQN}
import com.cdd.models.datatable.{DataTable, DenseVector, SparseVector, VectorBase}
import com.cdd.models.optimizer.{Blas, WeightedLeastSquares}
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.transformer.StandardScaler
import org.apache.commons.math3.stat.descriptive.SummaryStatistics

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait LinearRegressionParams extends HasPredictionParameters {
  val lambda = new Parameter[Double](this, "lambda", "Regularization weight")
  def setLambda(value:Double):this.type = setParameter(lambda, value)
  setDefaultParameter(lambda, 0.0)

  val elasticNetParam = new Parameter[Double](this, "elasticNetParam", "ElasticNet mixing parameter (0 = L2, 1 = L1)")
  def setElasticNetParam(value: Double):this.type = setParameter(elasticNetParam, value)
  setDefaultParameter(elasticNetParam, 0.0)

  val maxIter = new Parameter[Int](this, "maxIter", "Maximum number of iterations")
  def setMaxIter(value:Int):this.type = setParameter(maxIter, value)
  setDefaultParameter(maxIter, 100)

  val tolerance = new Parameter[Double](this, "tolerance", "Convergence tolerance of iterations")
  def setTolerance(value: Double): this.type  = setParameter(tolerance, value)
  setDefaultParameter(tolerance, 1e-6)
}

@SerialVersionUID(7206983407928290915L)
class LinearRegressor(val uid: String) extends Predictor[LinearRegressionModel]  with HasFeaturesAndLabelParameters
  with LinearRegressionParams {

  def this() = this(Identifiable.randomUID("lr"))

  override def fit(dataTable: DataTable): LinearRegressionModel = {

    val (labels, features) = extractFeaturesAndLabels(dataTable)
    val weights = None

  // Extract the number of features before deciding optimization solver.
    val numFeatures = features(0).length
    val fitIntercept = true
    val regParam = getParameter(lambda)
    val tol = getParameter(tolerance)
    val elasticNetParam = getParameter(this.elasticNetParam)
    val standardization = true
    val maxIter = getParameter(this.maxIter)

    // could make these variables parameters
    val solver = "auto"

    if (solver == "auto" &&
      (numFeatures <= WeightedLeastSquares.MAX_NUM_FEATURES) || solver == "normal") {
      // For low dimensional data, WeightedLeastSquares is more efficient since the
      // training algorithm only requires one pass through the data. (SPARK-10668)

      val optimizer = new WeightedLeastSquares(fitIntercept, regParam,
        elasticNetParam = elasticNetParam, standardization, true,
        solverType = WeightedLeastSquares.Auto, maxIter = maxIter, tol = tol)
      val model = optimizer.fit(labels, features, weights)
      // When it is trained by WeightedLeastSquares, training summary does not
      // attach returned model.
      val lrModel = copyParameterValues(new LinearRegressionModel(uid, model.coefficients, model.intercept))
      return lrModel
    }

    val featuresStatistics = StandardScaler.summaryStatisticsArrayForFeatures(features)
    val labelStatistics = labels.foldRight(new SummaryStatistics()) { (label, stats) =>
      stats.addValue(label)
      stats
    }

    val yMean = labelStatistics.getMean
    val rawYStd = math.sqrt(labelStatistics.getVariance)
    if (rawYStd == 0.0) {
      if (fitIntercept || yMean == 0.0) {
        // If the rawYStd==0 and fitIntercept==true, then the intercept is yMean with
        // zero coeOKfficient; as a result, training is not needed.
        // Also, if yMean==0 and rawYStd==0, all the coefficients are zero regardless of
        // the fitIntercept.
        if (yMean == 0.0) {
          logger.warn(s"Mean and standard deviation of the label are zero, so the coefficients " +
            s"and the intercept will all be zero; as a result, training is not needed.")
        } else {
          logger.warn(s"The standard deviation of the label is zero, so the coefficients will be " +
            s"zeros and the intercept will be the mean of the label; as a result, " +
            s"training is not needed.")
        }
        val coefficients = new SparseVector(Array(), Array(), numFeatures)
        val intercept = yMean

        val model = copyParameterValues(new LinearRegressionModel(uid, coefficients, intercept))
        return model
      } else {
        require(regParam == 0.0, "The standard deviation of the label is zero. " +
          "Model cannot be regularized.")
        logger.warn(s"The standard deviation of the label is zero. " +
          "Consider setting fitIntercept=true.")
      }
    }

    // if y is constant (rawYStd is zero), then y cannot be scaled. In this case
    // setting yStd=abs(yMean) ensures that y is not scaled anymore in l-bfgs algorithm.
    val yStd = if (rawYStd > 0) rawYStd else math.abs(yMean)
    val featuresMean = featuresStatistics.map(_.mean()).toVector
    val featuresStd = featuresStatistics.map(s => math.sqrt(s.variance())).toVector

    if (!fitIntercept && (0 until numFeatures).exists { i =>
      featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
      logger.warn("Fitting LinearRegressionModel without intercept on dataset with " +
        "constant nonzero column, will output zero coefficients for constant nonzero " +
        "columns. This behavior is the same as R glmnet but different from LIBSVM.")
    }

    // Since we implicitly do the feature scaling when we compute the cost function
    // to improve the convergence, the effective regParam will be changed.
    val effectiveRegParam = regParam / yStd
    val effectiveL1RegParam = elasticNetParam * effectiveRegParam
    val effectiveL2RegParam = (1.0 - elasticNetParam) * effectiveRegParam

    val costFun = new LeastSquaresCostFun(labels, features, weights, yStd, yMean, fitIntercept,
      standardization, featuresStd, featuresMean, effectiveL2RegParam)

    val optimizer = if (elasticNetParam == 0.0 || effectiveRegParam == 0.0) {
      new LBFGS[BreezeDenseVector[Double]](maxIter, 10, tol)
    } else {
      def effectiveL1RegFun = (index: Int) => {
        if (standardization) {
          effectiveL1RegParam
        } else {
          // If `standardization` is false, we still standardize the data
          // to improve the rate of convergence; as a result, we have to
          // perform this reverse standardization by penalizing each component
          // differently to get effectively the same objective function when
          // the training dataset is not standardized.
          if (featuresStd(index) != 0.0) effectiveL1RegParam / featuresStd(index) else 0.0
        }
      }
      new OWLQN[Int, BreezeDenseVector[Double]](maxIter, 10, effectiveL1RegFun, tol)
    }

    val initialCoefficients = new DenseVector(Array.ofDim[Double](numFeatures))
    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      new BreezeDenseVector(initialCoefficients.toArray()))

    val (coefficients, _) = {
      /*
         Note that in Linear Regression, the objective history (loss + regularization) returned
         from optimizer is computed in the scaled space given by the following formula.
         <blockquote>
            $$
            L &= 1/2n||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2
                 + regTerms \\
            $$
         </blockquote>
       */
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }
      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        logger.error(msg)
        throw new RuntimeException(msg)
      }

      /*
         The coefficients are trained in the scaled space; we're converting them back to
         the original space.
       */
      val rawCoefficients = state.x.toArray.clone()
      var i = 0
      val len = rawCoefficients.length
      while (i < len) {
        rawCoefficients(i) *= { if (featuresStd(i) != 0.0) yStd / featuresStd(i) else 0.0 }
        i += 1
      }

      (new DenseVector(rawCoefficients), arrayBuilder.result())
    }

    /*
       The intercept in R's GLMNET is computed using closed form after the coefficients are
       converged. See the following discussion for detail.
       http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
     */
    val intercept = if (fitIntercept) {
      yMean - Blas.dot(coefficients, new DenseVector(featuresMean.toArray))
    } else {
      0.0
    }

    copyParameterValues(new LinearRegressionModel(uid, coefficients, intercept))
  }
}

class LinearRegressionModel(val uid:String, val coefficients: VectorBase, val intercept:Double)
  extends PredictionModel[LinearRegressionModel] {

  override def transformRow(features: VectorBase): Double = {
    Blas.dot(features, coefficients) + intercept
  }
}


/**
 * LeastSquaresAggregator computes the gradient and loss for a Least-squared loss function,
 * as used in linear regression for samples in sparse or dense vector in an online fashion.
 *
 * Two LeastSquaresAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * For improving the convergence rate during the optimization process, and also preventing against
 * features with very large variances exerting an overly large influence during model training,
 * package like R's GLMNET performs the scaling to unit variance and removing the mean to reduce
 * the condition number, and then trains the model in scaled space but returns the coefficients in
 * the original scale. See page 9 in http://cran.r-project.org/web/packages/glmnet/glmnet.pdf
 *
 * However, we don't want to apply the `StandardScaler` on the training dataset, and then cache
 * the standardized dataset since it will create a lot of overhead. As a result, we perform the
 * scaling implicitly when we compute the objective function. The following is the mathematical
 * derivation.
 *
 * Note that we don't deal with intercept by adding bias here, because the intercept
 * can be computed using closed form after the coefficients are converged.
 * See this discussion for detail.
 * http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
 *
 * When training with intercept enabled,
 * The objective function in the scaled space is given by
 *
 * <blockquote>
 *    $$
 *    L = 1/2n ||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2,
 *    $$
 * </blockquote>
 *
 * where $\bar{x_i}$ is the mean of $x_i$, $\hat{x_i}$ is the standard deviation of $x_i$,
 * $\bar{y}$ is the mean of label, and $\hat{y}$ is the standard deviation of label.
 *
 * If we fitting the intercept disabled (that is forced through 0.0),
 * we can use the same equation except we set $\bar{y}$ and $\bar{x_i}$ to 0 instead
 * of the respective means.
 *
 * This can be rewritten as
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *     L &= 1/2n ||\sum_i (w_i/\hat{x_i})x_i - \sum_i (w_i/\hat{x_i})\bar{x_i} - y / \hat{y}
 *          + \bar{y} / \hat{y}||^2 \\
 *       &= 1/2n ||\sum_i w_i^\prime x_i - y / \hat{y} + offset||^2 = 1/2n diff^2
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where $w_i^\prime$ is the effective coefficients defined by $w_i/\hat{x_i}$, offset is
 *
 * <blockquote>
 *    $$
 *    - \sum_i (w_i/\hat{x_i})\bar{x_i} + \bar{y} / \hat{y}.
 *    $$
 * </blockquote>
 *
 * and diff is
 *
 * <blockquote>
 *    $$
 *    \sum_i w_i^\prime x_i - y / \hat{y} + offset
 *    $$
 * </blockquote>
 *
 * Note that the effective coefficients and offset don't depend on training dataset,
 * so they can be precomputed.
 *
 * Now, the first derivative of the objective function in scaled space is
 *
 * <blockquote>
 *    $$
 *    \frac{\partial L}{\partial w_i} = diff/N (x_i - \bar{x_i}) / \hat{x_i}
 *    $$
 * </blockquote>
 *
 * However, $(x_i - \bar{x_i})$ will densify the computation, so it's not
 * an ideal formula when the training dataset is sparse format.
 *
 * This can be addressed by adding the dense $\bar{x_i} / \hat{x_i}$ terms
 * in the end by keeping the sum of diff. The first derivative of total
 * objective function from all the samples is
 *
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *       \frac{\partial L}{\partial w_i} &=
 *           1/N \sum_j diff_j (x_{ij} - \bar{x_i}) / \hat{x_i} \\
 *         &= 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) - diffSum \bar{x_i} / \hat{x_i}) \\
 *         &= 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) + correction_i)
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * where $correction_i = - diffSum \bar{x_i} / \hat{x_i}$
 *
 * A simple math can show that diffSum is actually zero, so we don't even
 * need to add the correction terms in the end. From the definition of diff,
 *
 * <blockquote>
 *    $$
 *    \begin{align}
 *       diffSum &= \sum_j (\sum_i w_i(x_{ij} - \bar{x_i})
 *                    / \hat{x_i} - (y_j - \bar{y}) / \hat{y}) \\
 *         &= N * (\sum_i w_i(\bar{x_i} - \bar{x_i}) / \hat{x_i} - (\bar{y} - \bar{y}) / \hat{y}) \\
 *         &= 0
 *    \end{align}
 *    $$
 * </blockquote>
 *
 * As a result, the first derivative of the total objective function only depends on
 * the training dataset, which can be easily computed in distributed fashion, and is
 * sparse format friendly.
 *
 * <blockquote>
 *    $$
 *    \frac{\partial L}{\partial w_i} = 1/N ((\sum_j diff_j x_{ij} / \hat{x_i})
 *    $$
 * </blockquote>
 *
 * @param coefficients The coefficients corresponding to the features.
 * @param labelStd The standard deviation value of the label.
 * @param labelMean The mean value of the label.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
@SerialVersionUID(1000L)
private class LeastSquaresAggregator(
    labels: Vector[Double], features: Vector[VectorBase], weights: Option[Vector[Double]],
    coefficients: Vector[Double],
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    featuresStd: Vector[Double],
    featuresMean: Vector[Double]) extends Serializable {

  private var totalCnt: Long = 0L
  private var weightSum: Double = 0.0
  private var lossSum = 0.0

  private val dim = coefficients.size

  val coefficientsArray = ArrayBuffer(coefficients:_*)
  var sum = 0.0
  var i = 0
  val len = coefficientsArray.length
  while (i < len) {
    if (featuresStd(i) != 0.0) {
      coefficientsArray(i) /=  featuresStd(i)
      sum += coefficientsArray(i) * featuresMean(i)
    } else {
      coefficientsArray(i) = 0.0
    }
    i += 1
  }
  val offset = if (fitIntercept) labelMean / labelStd - sum else 0.0

  val effectiveCoefficientsVector = new DenseVector(coefficientsArray.toArray)
  val gradientSumArray = Array.ofDim[Double](dim)

  labels.zip(features).zipWithIndex.foreach { case ((label, instanceFeatures), i) =>
    val weight = weights match {
      case Some(warr) => warr(i)
      case None => 1.0
    }

    if (weight != 0.0) {

      val diff = Blas.dot(instanceFeatures, effectiveCoefficientsVector) - label / labelStd + offset

      if (diff != 0) {
        val localGradientSumArray = gradientSumArray
        val localFeaturesStd = featuresStd
        instanceFeatures.forEachActive { (index, value) =>
          if (localFeaturesStd(index) != 0.0 && value != 0.0) {
            localGradientSumArray(index) += weight * diff * value / localFeaturesStd(index)
          }
        }
        lossSum += weight * diff * diff / 2.0
      }

      totalCnt += 1
      weightSum += weight

    }
  }

  def count: Long = totalCnt

  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum / weightSum
  }

  def gradient: DenseVector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    val result = new DenseVector(gradientSumArray.clone())
    Blas.scal(1.0 / weightSum, result)
    result
  }
}


/**
 * LeastSquaresCostFun implements Breeze's DiffFunction[T] for Least Squares cost.
 * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
 * It's used in Breeze's convex optimization routines.
 */
private class LeastSquaresCostFun(
    labels: Vector[Double], features: Vector[VectorBase], weights: Option[Vector[Double]],
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    standardization: Boolean,
    featuresStd: Vector[Double],
    featuresMean: Vector[Double],
    effectiveL2regParam: Double) extends DiffFunction[BreezeDenseVector[Double]] {

  override def calculate(coefficients: BreezeDenseVector[Double]): (Double, BreezeDenseVector[Double]) = {
    val coeffs = new DenseVector(coefficients.toArray)

    val leastSquaresAggregator = new LeastSquaresAggregator(
      labels, features, weights,
      coefficients.toArray.toVector, labelStd, labelMean, fitIntercept, featuresStd,
      featuresMean)

    val totalGradientArray = leastSquaresAggregator.gradient.toArray

    val regVal = if (effectiveL2regParam == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.forEachActive { (index, value) =>
        // The following code will compute the loss of the regularization; also
        // the gradient of the regularization, and add back to totalGradientArray.
        sum += {
          if (standardization) {
            totalGradientArray(index) += effectiveL2regParam * value
            value * value
          } else {
            if (featuresStd(index) != 0.0) {
              // If `standardization` is false, we still standardize the data
              // to improve the rate of convergence; as a result, we have to
              // perform this reverse standardization by penalizing each component
              // differently to get effectively the same objective function when
              // the training dataset is not standardized.
              val temp = value / (featuresStd(index) * featuresStd(index))
              totalGradientArray(index) += effectiveL2regParam * temp
              value * temp
            } else {
              0.0
            }
          }
        }
      }
      0.5 * effectiveL2regParam * sum
    }

    (leastSquaresAggregator.loss + regVal, new BreezeDenseVector[Double](totalGradientArray))
  }
}
