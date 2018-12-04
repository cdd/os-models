package com.cdd.models.vault

import com.cdd.models.datatable._
import com.cdd.models.molecule.RdkitDeMorganFingerprinter.RdkitFingerprintClass
import com.cdd.models.molecule._
import com.cdd.models.utils.HasLogging
import org.RDKit.MolSanitizeException
import org.openscience.cdk.fingerprint.CircularFingerprinter
import org.openscience.cdk.silent.SilentChemObjectBuilder
import org.openscience.cdk.smiles.{SmiFlavor, SmilesGenerator, SmilesParser}

@SerialVersionUID(1000L)
class MoleculeAndFeatures(val id: Int, val smiles: String, val rdkitSmiles: String, val cdkSmiles: String, val rdkitFp: SparseVector,
                          val cdkFp: SparseVector, val rdkitDesc: DenseVector, val cdkDesc: DenseVector) extends Serializable

object MoleculeAndFeatures extends HasLogging {
  def buildMolecules(molsIn: Vector[MoleculeSummary]): Vector[Option[MoleculeAndFeatures]] = {

    val smilesParser = new SmilesParser(SilentChemObjectBuilder.getInstance())
    val circ = new CircularFingerprinter(CdkCircularFingerprinter.fingerprintClassToCdkClass(CdkCircularFingerprinter.CdkFingerprintClass.FCFP6))

    val smilesGenerator = new SmilesGenerator(SmiFlavor.Absolute)
    val rdkitFingerprinter = new RdkitDeMorganFingerprinter(RdkitFingerprintClass.FCFP6)

    val rdkitPropertyCalculator = new RdkitDescriptors
    val cdkPropertyCalculator = new CdkDescriptors

    val molsOut = molsIn.zipWithIndex.map { case (m, i) =>
      if (i % 500 == 0)
        logger.info(s"Processed $i smiles")
      val smiles = m.smiles
      var rdkitFp: SparseVector = null
      var rdkitSmiles: String = null
      var cdkSmiles: String = null
      var cdkFp: SparseVector = null
      var rdkitDesc: DenseVector = null
      var cdkDesc: DenseVector = null
      if (smiles != null) {
        val rdkMol = RdkitUtils.smilesToMol(smiles)
        if (rdkMol.isDefined) {
          try {
            rdkitSmiles = rdkMol.get.MolToSmiles()
            val fp = rdkitFingerprinter.fingerprint(rdkMol.get)
            rdkitFp = RdkitDeMorganFingerprinter.fingerprintToSparse(fp)
            val mlVector = rdkitPropertyCalculator.physicalProperties(rdkMol.get)
            mlVector.map(v => Vectors.Dense(v.toArray)) match {
              case Some(v) => rdkitDesc = v
              case _ =>
            }
          } catch {
            case ex: MolSanitizeException =>
              logger.warn("Exception while processing CDK molecule", ex)
          }
        }

        try {
          val cdkMol = smilesParser.parseSmiles(smiles)
          cdkSmiles = smilesGenerator.create(cdkMol)
          cdkFp = CdkTableBuilderFunctions.fingerprintMolecule(cdkMol, circ)
          CdkDescriptors.synchronized {
            val mlVector = cdkPropertyCalculator.descriptors(cdkMol)
            mlVector.map(v => Vectors.Dense(v.toArray)) match {
              case Some(v) => cdkDesc = v
              case _ =>
            }
          }
        } catch {
          case ex: Exception => logger.warn("Exception while processing CDK molecule", ex)
        }
      }

      if (smiles != null && rdkitSmiles != null && cdkSmiles != null && rdkitFp != null && cdkFp != null &&
        rdkitDesc != null && cdkDesc != null) {
        val molecule = new MoleculeAndFeatures(m.id, smiles, rdkitSmiles, cdkSmiles, rdkitFp, cdkFp, rdkitDesc, cdkDesc)
        Some(molecule)
      } else {
        None
      }
    }

    molsOut
  }

}
