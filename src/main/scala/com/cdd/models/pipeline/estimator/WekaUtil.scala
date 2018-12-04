package com.cdd.models.pipeline.estimator

import com.cdd.models.datatable.{DenseVector, SparseVector, VectorBase}
import weka.core._
import java.util

import com.cdd.models.pipeline.{HasParameters, Parameter}
import weka.classifiers.Classifier
import weka.filters.Filter
import weka.filters.unsupervised.attribute.NumericToNominal

import scala.collection.JavaConverters._

object WekaUtil {

  def featuresAndLabelsToBinaryInstances(labels: Vector[Double], features: Vector[VectorBase], classification: Boolean = true): Instances = {
    val numInstances = labels.length
    val numFeatures = features(0).length()
    var attributes = (1 to numFeatures).map { no =>
      new Attribute(s"feature_$no", Seq("0", "1").asJava)
    }
    val labelAttribute = classification match {
      case true => new Attribute("Label", Seq("inactive", "active").asJava)
      case false => new Attribute("Label")
    }

    attributes = attributes :+ labelAttribute
    val instances = new Instances("Instances", new util.ArrayList[Attribute](attributes.asJava), numInstances)

    features.zip(labels).foreach { case (f, l) =>
      val instance = f match {
        case v: SparseVector =>
          val newValues = v.values().map( v => if (v>0) 1.0 else 0.0)
          new SparseInstance(1.0, newValues :+ l, v.indices() :+ v.length(), v.length() + 1)
        case v: DenseVector =>
          val newValues = v.toArray().map( v => if (v>0) 1.0 else 0.0)
          new DenseInstance(1.0, newValues :+ l)
        case _ => throw new IllegalArgumentException("Unknown feature type")
      }
      instances.add(instance)
    }

    assert(instances.numAttributes() == numFeatures + 1)
    assert(instances.numInstances() == numInstances)
    instances.setClassIndex(numFeatures)
    instances
  }

  def featuresAndLabelsToInstances(labels: Vector[Double], features: Vector[VectorBase],
                                   classification: Boolean = false, nominalFeaturesCutoff: Int = -1): Instances = {
    val numInstances = labels.length
    val numFeatures = features(0).length()
    var attributes = (1 to numFeatures).map { no =>
      new Attribute(s"feature_$no")
    }
    val labelAttribute = classification match {
      case true => new Attribute("Label", Seq("inactive", "active").asJava)
      case false => new Attribute("Label")
    }
    attributes = attributes :+ labelAttribute
    val instances = new Instances("Instances", new util.ArrayList[Attribute](attributes.asJava), numInstances)

    features.zip(labels).foreach { case (f, l) =>
      val instance = f match {
        case v: SparseVector =>
          new SparseInstance(1.0, v.values() :+ l, v.indices() :+ v.length(), v.length() + 1)
        case v: DenseVector =>
          new DenseInstance(1.0, v.toArray() :+ l)
        case _ => throw new IllegalArgumentException("Unknown feature type")
      }
      instances.add(instance)
    }

    assert(instances.numAttributes() == numFeatures + 1)
    assert(instances.numInstances() == numInstances)
    instances.setClassIndex(numFeatures)

    if (nominalFeaturesCutoff > 0) {
      val nominalFeatures = (0 until numFeatures).toArray.filter { index =>
        instances.attributeToDoubleArray(index).toSet.size <= nominalFeaturesCutoff
      }
      val numericToNominal = new NumericToNominal
      numericToNominal.setAttributeIndicesArray(nominalFeatures)
      numericToNominal.setInputFormat(instances)

      Filter.useFilter(instances, numericToNominal)
    } else {
      instances
    }
  }

  def predict(estimator: Classifier, header: Instances, features: VectorBase): Double = {
    val instance = predictionInstance(header, features)
    estimator.classifyInstance(instance)
  }

  def classificationWithProbability(estimator: Classifier, header: Instances, features: VectorBase,
                                    binary:Boolean=false): (Double, Double) = {
    val instance = predictionInstance(header, features, binary)
    val category = estimator.classifyInstance(instance)
    val probabilities = estimator.distributionForInstance(instance)
    assert(probabilities.length == 2)
    assert(category == 0.0 || category == 1.0)
    val probability = probabilities(1)
    assert(probability >= 0.0 && probability <= 1.0)
    category match {
      case 0.0 => assert(probability <= 0.5)
      case 1.0 => assert(probability >= 0.5)
    }
    (category, probability)
  }

  private def predictionInstance(header: Instances, features: VectorBase, binary:Boolean=false): Instance = {
    val instance = features match {
      case v: SparseVector =>
        val newValues = v.values.zip(v.indices()).map { case (v, i) =>
          instanceValue(header, v, i, binary)
        }
        new SparseInstance(1.0, newValues, v.indices(), v.length())
      case v: DenseVector =>
        val newValues = v.toArray().zipWithIndex.map { case (v, i) =>
          instanceValue(header, v, i, binary)
        }
        new DenseInstance(1.0, newValues)
      case _ => throw new IllegalArgumentException("Unknown feature type")
    }
    instance.setDataset(header)
    instance
  }


  private def instanceValue(header: Instances, value: Double, index: Int, binary:Boolean): Double = {
    val attr = header.attribute(index)
    if (binary)
      require(attr.isNominal)
    if (!attr.isNominal)
      return value
    else if (binary) {
      if (value > 0.0) 1.0 else 0.0
    }
    else {
      val nominalValueIndex = attr.enumerateValues().asScala.indexWhere {
        _ match {
          case nv: String => nv.toDouble == value
          case _ => throw new IllegalStateException()
        }
      }
      if (nominalValueIndex == -1)
        Double.NaN
      else
        nominalValueIndex.toDouble
    }
  }
}


trait HasCommonWekaParameters extends HasParameters {
  val nominalFeaturesCutoff = new Parameter[Int](this, "nominalFeaturesCutoff", "Maximum number of feature values for nominal attributes")

  def setNominalFeaturesCutoff(value: Int): this.type = setParameter(nominalFeaturesCutoff, value)

  setDefaultParameter(nominalFeaturesCutoff, -1)
}
