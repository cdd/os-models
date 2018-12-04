package com.cdd.spark.pipeline


import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass
import com.cdd.models.molecule.CdkCircularFingerprinter.CdkFingerprintClass.CdkFingerprintClass
import com.cdd.models.molecule.{CdkCircularFingerprinter, CdkDescriptors, CdkMoleculeReader}
import com.cdd.models.utils.{HasLogging, TestSdfileProperties}
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.openscience.cdk.fingerprint.CircularFingerprinter
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.silent.SilentChemObjectBuilder
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator, SmilesParser}

import scala.collection.mutable
import scala.reflect.runtime.universe._


/**
  * Created by gjones on 7/5/17.
  */
class CdkScalableInputBuilder(path: String, activityFields: List[TestSdfileProperties], resource: Boolean, addActivities: Boolean)
  extends ScalableInputBuilder[CdkScalableInputBuilder, IAtomContainer](path, activityFields, resource, addActivities, "cdk")
    with HasLogging {

  override def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        val smiGen = new SmilesGenerator(SmiFlavor.Absolute)
        var smiles = smiGen.create(m)
        if (smiles.isEmpty)
          smiles = null
        Some(MoleculeInfo(moleculeNo, smiles))
      case None => None
    }
  }

  override protected def activityFieldValue(moleculeNo: Int, field: String): Option[String] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        CdkMoleculeReader.extractProperty(field, m)
      case None => None
    }
  }

  override def loadMolecules() = {
    CdkMoleculeReader.readMolecules(path, resource)
  }

  protected def addColumnFromSmiles[T: TypeTag](name: String, func: String => T): this.type = {
    val sqlfunc = functions.udf(func)
    val df = this.df.get.withColumn(name, sqlfunc(functions.col("cdk_smiles")))
    this.df = Some(df)
    this
  }

  override def addFingerprintColumns(): this.type = {

    val func: (CdkFingerprintClass, String) => Vector = (fingerprintClass, smiles) => {
      smiles match {
        case null => null
        case "" => null
        case smiles =>
          val circ = new CircularFingerprinter(CdkCircularFingerprinter.fingerprintClassToCdkClass(fingerprintClass))
          val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
          val molecule = smilesParser.parseSmiles(smiles)
          circ.calculate(molecule)
          val counts = mutable.Map.empty[Int, Double].withDefaultValue(.0)
          (0 until circ.getFPCount).map {
            circ.getFP(_)
          }.foreach { fp =>
            fp.hashCode match {
              case x if x >= 0 => counts(x) += 1.0
              case x =>
                logger.debug("Negative fingerprint hash- folding")
                counts(-x) += 1.0
            }
          }
          Vectors.sparse(Int.MaxValue, counts.toSeq)
      }
    }

    addColumnFromSmiles("fingerprints_CDK_ECFP6", func(CdkFingerprintClass.ECFP6, _: String))
    addColumnFromSmiles("fingerprints_CDK_FCFP6", func(CdkFingerprintClass.FCFP6, _: String))
    this
  }

  override def addDescriptorColumns(): this.type = {
    val func: (String => Vector) = (smiles) => {
      smiles match {
        case null => null
        case "" => null
        case smiles =>
          CdkDescriptors.synchronized {
            val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
            val molecule = smilesParser.parseSmiles(smiles)
            val propertyCalculator = new CdkDescriptors
            propertyCalculator.descriptors(molecule) match {
              case Some(v) => v
              case _ => null
            }
          }
      }
    }

    addColumnFromSmiles("cdk_descriptors", func)
    this
  }
}
