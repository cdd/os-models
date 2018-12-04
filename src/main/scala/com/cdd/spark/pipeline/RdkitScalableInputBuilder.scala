package com.cdd.spark.pipeline

import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass.RdkitFingerprintClass
import com.cdd.models.molecule.{RdkitDeMorganFingerprinter, RdkitDescriptors, RdkitMoleculeReader}
import com.cdd.models.utils.{HasLogging, TestSdfileProperties}
import org.RDKit.{ROMol, RWMol}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.mutable
import scala.reflect.runtime.universe._


class RdkitScalableInputBuilder(path: String, activityFields: List[TestSdfileProperties], resource: Boolean, addActivities: Boolean)
  extends ScalableInputBuilder[RdkitScalableInputBuilder, ROMol](path, activityFields, resource, addActivities, "rdkit") with HasLogging {


  override def moleculeInfo(moleculeNo: Int): Option[MoleculeInfo] = {
    molecules(moleculeNo) map { m =>
        var smiles = m.MolToSmiles(true)
        if (smiles.isEmpty)
          smiles = null
        MoleculeInfo(moleculeNo, smiles)
    }
  }

  override  def loadMolecules() = {
    RdkitMoleculeReader.readMolecules(path, resource)
  }

  override protected def activityFieldValue(moleculeNo: Int, field: String): Option[String] = {
    molecules(moleculeNo) match {
      case Some(m) =>
        RdkitMoleculeReader.extractProperty(field, m)
      case None => None
    }
  }

  protected def addColumnFromSmiles[T: TypeTag](name: String, func: String => T): this.type = {
    val sqlfunc = functions.udf(func)
    val df = this.df.get.withColumn(name, sqlfunc(functions.col("rdkit_smiles")))
    this.df = Some(df)
    this
  }

  override def addFingerprintColumns(): this.type = {

    val func: (RdkitFingerprintClass, String) => Vector = (fingerprintClass, smiles) => {
      smiles match {
        case null => null
        case smiles =>
          val molecule = RWMol.MolFromSmiles(smiles)
          assert(molecule != null)
          val fingerprinter = new RdkitDeMorganFingerprinter(fingerprintClass)
          val fp = fingerprinter.fingerprint(molecule)
          val counts = mutable.Map.empty[Int, Double].withDefaultValue(.0)
          fp.foreach { case (index, value) =>
            index match {
              case i if i >= 0 && i <= Int.MaxValue => counts(i.toInt) += value
              case i if i > Int.MaxValue =>
                val intIndex = (i - Int.MaxValue).toInt
                logger.debug(s"Long fingerprint hash- folding ${i} to ${intIndex}")
                counts(intIndex) += value
            }
          }

          Vectors.sparse(Int.MaxValue, counts.toSeq)
      }
    }


    addColumnFromSmiles("fingerprints_RDKit_ECFP6", func(RdkitFingerprintClass.ECFP6, _: String))
    addColumnFromSmiles("fingerprints_RDKit_FCFP6", func(RdkitFingerprintClass.FCFP6, _: String))
    this
  }

  override def addDescriptorColumns(): this.type = {
    val func: (String => Vector) = (smiles) => {
      smiles match {
        case null => null
        case "" => null
        case smiles =>
          val molecule = RWMol.MolFromSmiles(smiles)
          assert(molecule != null)
          val propertyCalculator = new RdkitDescriptors
          propertyCalculator.physicalProperties(molecule) match {
            case Some(v) => v
            case _ => null
          }
      }
    }

    addColumnFromSmiles("rdkit_descriptors", func)
    this
  }
}
