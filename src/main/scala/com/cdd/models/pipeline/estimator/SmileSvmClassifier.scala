package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DataTable, VectorBase}
import com.cdd.models.pipeline.{Identifiable, PredictionModelWithProbabilities, PredictorWithProbabilities}
import smile.math.SparseArray
import smile.classification.SVM
import smile.math.kernel.MercerKernel

object SmileSvmClassifier {
  // replaces smiles code in Operators.scala to pass maxIter to our customized svm
  def svm[T <: AnyRef](x: Array[T], y: Array[Int], kernel: MercerKernel[T], C: Double, strategy: SVM.Multiclass = SVM.Multiclass.ONE_VS_ONE, epoch: Int = 1, maxIter:Int = 1): SVM[T] = {
    val k = y.max + 1
    require(k==2)
    val svm = if (k == 2) new SVM[T](kernel, C, maxIter) else new SVM[T](kernel, C, k, strategy, maxIter)
      for (i <- 1 to epoch) {
        println(s"SVM training epoch $i...")
        svm.learn(x, y)
        svm.finish
      }
    svm
  }
}

class SmileSvmClassifier(val uid: String)
  extends PredictorWithProbabilities[SmileSvmClassificationModel]
    with HasSmileSvmParameters {
  import SmileSvmClassifier._

  def this() = this(Identifiable.randomUID("smile_svc"))

  override def fit(dataTable: DataTable): SmileSvmClassificationModel = {
    val (_, labels, denseFeatures, sparseFeatures) = extractFeaturesAndLabelsToSmileArrays(dataTable)
    var sparseSvc: Option[SVM[SparseArray]] = None
    var denseSvc: Option[SVM[Array[Double]]] = None
    val intLabels = labels.map(_.toInt)

    if (sparseFeatures != None) {
      val kernel = sparseKernel()
      val svc = svm(sparseFeatures.get.toArray, intLabels, kernel, getParameter(C), maxIter = getParameter(maxIter))
      svc.trainPlattScaling(sparseFeatures.get.toArray, intLabels)
      sparseSvc = Some(svc)
    } else {
      val kernel = denseKernel()
      val svc = svm(denseFeatures.get, intLabels, kernel, getParameter(C), maxIter = getParameter(maxIter))
      svc.trainPlattScaling(denseFeatures.get, intLabels)
      denseSvc = Some(svc)
    }

    copyParameterValues(new SmileSvmClassificationModel(uid, sparseSvc, denseSvc))
  }

}

@SerialVersionUID(1000L)
class SmileSvmClassificationModel(override val uid: String, sparseSvc: Option[SVM[SparseArray]],
                                  denseSvc: Option[SVM[Array[Double]]])
  extends PredictionModelWithProbabilities[SmileSvmClassificationModel] with HasSmileSvmParameters {

  val probabilities = new Array[Double](2)

  override def transformRow(features: VectorBase): (Double, Double) = {
    val category = if (sparseSvc != None) {
      sparseSvc.get.predict(SmileUtil.vectorToSparseArray(features), probabilities)
    } else {
      denseSvc.get.predict(features.toArray(), probabilities)
    }
    val probability = probabilities(1)
    /*
    if (category == 1) {
      assert(probability >= 0.5)
    }
    else {
      assert(probability <= 1.0)
    }
    */
    (category.toDouble, probability)
  }
}
