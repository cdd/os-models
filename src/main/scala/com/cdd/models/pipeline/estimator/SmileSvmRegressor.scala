package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline._
import smile.math.SparseArray
import smile.regression.SVR
import smile.regression._

trait HasSmileSvmParameters extends HasParameters with SmileMercerKernel with HasFeaturesAndLabelParameters {

  val C = new Parameter[Double](this, "C", "soft margin penalty parameter")

  def setC(value: Double): this.type = setParameter(C, value)

  setDefaultParameter(C, 1.0)

  val maxIter = new Parameter[Int](this, "maxIter", "Maximum number of SVR training iterations (-1 for unlimited)")

  def setMaxIter(value: Int): this.type = setParameter(maxIter, value)

  setDefaultParameter(maxIter, -1)
}

trait HasSmileSvrParameters extends HasSmileSvmParameters {

  val eps = new Parameter[Double](this, "eps", "loss function error threshold (-1.0 to guess from label values)")

  def setEps(value: Double): this.type = setParameter(eps, value)

  //setDefaultParameter(eps, 0.1)
  setDefaultParameter(eps, -1.0)

  val tol = new Parameter[Double](this, "tol", "the tolerance of the convergence test (-1.0 to guess from label values)")

  def setTol(value: Double): this.type = setParameter(tol, value)

  //setDefaultParameter(tol, 0.001)
  setDefaultParameter(tol, -1.0)

}

@SerialVersionUID(881189128209737190L)
class SmileSvmRegressor(val uid: String) extends Predictor[SmileSvmRegressionModel]
  with HasSmileSvrParameters {
  def this() = this(Identifiable.randomUID("smile_svr"))

  override def fit(dataTable: DataTable): SmileSvmRegressionModel = {

    val (_, labels, denseFeatures, sparseFeatures) = extractFeaturesAndLabelsToSmileArrays(dataTable)
    var sparseSvr: Option[SVR[SparseArray]] = None
    var denseSvr: Option[SVR[Array[Double]]] = None
    var tol = getParameter(this.tol)
    val labelDimensionality = this.labelDimensionality(dataTable)
    if (tol == -1.0) {
      tol = labelDimensionality * 0.000001
      if (tol > 0.001)
        tol = 0.001
      else
        logger.info(s"Adjusting tolerance parameter to $tol")
    }
    val c = getParameter(C)

    var eps = getParameter(this.eps)
    if(eps == -1.0) {
      eps = labelDimensionality/20
      if (eps > .1)
        eps =   0.1
      else
        logger.info(s"Adjusting soft margin parameter to $eps")
    }


    if (sparseFeatures != None) {
      val kernel = sparseKernel()
      val svr = new SVR[SparseArray](sparseFeatures.get.toArray, labels, null, kernel, eps,
        c, tol, getParameter(maxIter))
      sparseSvr = Some(svr)
    } else {
      val kernel = denseKernel()
      val svr = new SVR[Array[Double]](denseFeatures.get, labels, null, kernel, eps,
        c, tol, getParameter(maxIter))
      denseSvr = Some(svr)
      //denseSvr = Some(svr(denseFeatures.get, labels, kernel, getParameter(eps), getParameter(C), null, getParameter(tol)))
    }

    copyParameterValues(new SmileSvmRegressionModel(uid, sparseSvr, denseSvr))
  }
}

class SmileSvmRegressionModel(val uid: String, sparseSvr: Option[SVR[SparseArray]],
                              denseSvr: Option[SVR[Array[Double]]])
  extends PredictionModel[SmileSvmRegressionModel] with HasSmileSvrParameters {


  override def transformRow(features: VectorBase): Double = {
    if (sparseSvr != None) {
      sparseSvr.get.predict(SmileUtil.vectorToSparseArray(features))
    } else {
      denseSvr.get.predict(features.toArray())
    }
  }
}