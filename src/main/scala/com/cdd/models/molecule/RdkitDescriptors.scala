package com.cdd.models.molecule

import com.cdd.models.utils.HasLogging
import org.RDKit.{RDKFuncs, ROMol}
import org.apache.spark.ml.linalg.{Vector, Vectors}


object RdkitDescriptors extends HasLogging{

  object RdkitDescriptorType extends Enumeration {
    type RdkitPhysicalPropertyType = Value
    val MOL_LOGP, MOL_CMR, CRIPPEN_LOGP, CRIPPEN_CMR, TPSA, NUM_HBA, NUM_HBD, LIPINSKI_HBA, LIPINSKI_HBD,
    SLOGP_VSA1, SLOGP_VSA2, SLOGP_VSA3, SLOGP_VSA4, SLOGP_VSA5, SLOGP_VSA6, SLOGP_VSA7, SLOGP_VSA8, SLOGP_VSA9,
    SLOGP_VSA10, SLOGP_VSA11, SLOGP_VSA12,
    PEOE_VSA1, PEOE_VSA2, PEOE_VSA3, PEOE_VSA4, PEOE_VSA5, PEOE_VSA6, PEOE_VSA7, PEOE_VSA8, PEOE_VSA9, PEOE_VSA10,
    PEOE_VSA11, PEOE_VSA12, PEOE_VSA13, PEOE_VSA14,
    SMR_VSA1, SMR_VSA2, SMR_VSA3, SMR_VSA4, SMR_VSA5, SMR_VSA6, SMR_VSA7, SMR_VSA8, SMR_VSA9, SMR_VSA10,
    MOL_AMW, HALL_KIER_ALPHA, KAPPA_1, KAPPA_2, KAPPA_3,
    CHI_0N, CHI_0V, CHI_1N, CHI_1V, CHI_2N, CHI_2V, CHI_3N, CHI_3V, CHI_4N, CHI_4V,
    EXACT_MW, FRACTION_CSP3,
    NUM_ROTATABLE_BONDS, NUM_HETEROATOMS, NUM_AMIDE_BONDS, NUM_RINGS, NUM_AROMATIC_RINGS, NUM_ALIPHATIC_RINGS,
    NUM_SATURATED_RINGS, NUM_HETEROCYCLES, NUM_AROMATIC_HETEROCYCLES,
    NUM_AROMATIC_CARBOCYCLES, LABUTE_ASA,
    MQN1, MQN2, MQN3, MQN4, MQN5, MQN6, MQN7, MQN8, MQN9, MQN10, MQN11, MQN12, MQN13, MQN14, MQN15, MQN16, MQN17, MQN18,
    MQN19, MQN20, MQN21, MQN22, MQN23, MQN24, MQN25, MQN26, MQN27, MQN28, MQN29, MQN30, MQN31, MQN32, MQN33, MQN34, MQN35,
    MQN36, MQN37, MQN38, MQN39, MQN40, MQN41, MQN42 = Value
  }

 def descriptorsForMolecules(molecules: List[Option[ROMol]]): List[Option[Vector]] = {
   val propertyCalculator = new RdkitDescriptors
    molecules.map {
      case Some(molecule) => propertyCalculator.physicalProperties(molecule)
      case None => None
    }
  }

}

import com.cdd.models.molecule.RdkitDescriptors.RdkitDescriptorType
import com.cdd.models.molecule.RdkitDescriptors.RdkitDescriptorType._
import com.cdd.models.molecule.RdkitDescriptors.logger

/**
  * Created by gjones on 5/31/17.
  */
class RdkitDescriptors extends HasLogging{


  def physicalProperties(molecule: ROMol): Option[Vector] = {

    val descriptors = Array.fill[Double](RdkitDescriptorType.maxId)(Double.NaN)

    descriptors(NUM_HBA.id) = RDKFuncs.calcNumHBA(molecule)
    descriptors(NUM_HBD.id) = RDKFuncs.calcNumHBD(molecule)
    val pair = RDKFuncs.calcCrippenDescriptors(molecule)
    descriptors(CRIPPEN_LOGP.id) = pair.getFirst
    descriptors(CRIPPEN_CMR.id) = pair.getSecond

    descriptors(LIPINSKI_HBA.id) = RDKFuncs.calcLipinskiHBA(molecule)
    descriptors(LIPINSKI_HBD.id) = RDKFuncs.calcLipinskiHBD(molecule)

    descriptors(MOL_LOGP.id) = RDKFuncs.calcMolLogP(molecule)
    descriptors(MOL_CMR.id) = RDKFuncs.calcMolMR(molecule)

    descriptors(TPSA.id) = RDKFuncs.calcTPSA(molecule)

    var values = RDKFuncs.calcSlogP_VSA(molecule)
    descriptors(SLOGP_VSA1.id) = values.get(0)
    descriptors(SLOGP_VSA2.id) = values.get(1)
    descriptors(SLOGP_VSA3.id) = values.get(2)
    descriptors(SLOGP_VSA4.id) = values.get(3)
    descriptors(SLOGP_VSA5.id) = values.get(4)
    descriptors(SLOGP_VSA6.id) = values.get(5)
    descriptors(SLOGP_VSA7.id) = values.get(6)
    descriptors(SLOGP_VSA8.id) = values.get(7)
    descriptors(SLOGP_VSA9.id) = values.get(8)
    descriptors(SLOGP_VSA10.id) = values.get(9)
    descriptors(SLOGP_VSA11.id) = values.get(10)
    descriptors(SLOGP_VSA12.id) = values.get(11)

    values = RDKFuncs.calcPEOE_VSA(molecule)
    descriptors(PEOE_VSA1.id) = values.get(0)
    descriptors(PEOE_VSA2.id) = values.get(1)
    descriptors(PEOE_VSA3.id) = values.get(2)
    descriptors(PEOE_VSA4.id) = values.get(3)
    descriptors(PEOE_VSA5.id) = values.get(4)
    descriptors(PEOE_VSA6.id) = values.get(5)
    descriptors(PEOE_VSA7.id) = values.get(6)
    descriptors(PEOE_VSA8.id) = values.get(7)
    descriptors(PEOE_VSA9.id) = values.get(8)
    descriptors(PEOE_VSA10.id) = values.get(9)
    descriptors(PEOE_VSA11.id) = values.get(10)
    descriptors(PEOE_VSA12.id) = values.get(11)
    descriptors(PEOE_VSA13.id) = values.get(12)
    descriptors(PEOE_VSA14.id) = values.get(13)

    values = RDKFuncs.calcSMR_VSA(molecule)
    descriptors(SMR_VSA1.id) = values.get(0)
    descriptors(SMR_VSA2.id) = values.get(1)
    descriptors(SMR_VSA3.id) = values.get(2)
    descriptors(SMR_VSA4.id) = values.get(3)
    descriptors(SMR_VSA5.id) = values.get(4)
    descriptors(SMR_VSA6.id) = values.get(5)
    descriptors(SMR_VSA7.id) = values.get(6)
    descriptors(SMR_VSA8.id) = values.get(7)
    descriptors(SMR_VSA9.id) = values.get(8)
    descriptors(SMR_VSA10.id) = values.get(9)

    descriptors(MOL_AMW.id) = RDKFuncs.calcAMW(molecule)
    descriptors(HALL_KIER_ALPHA.id) = RDKFuncs.calcHallKierAlpha(molecule)

    descriptors(KAPPA_1.id) = RDKFuncs.calcKappa1(molecule)
    descriptors(KAPPA_2.id) = RDKFuncs.calcKappa2(molecule)
    descriptors(KAPPA_3.id) = RDKFuncs.calcKappa3(molecule)

    descriptors(CHI_0N.id) = RDKFuncs.calcChi0n(molecule)
    descriptors(CHI_0V.id) = RDKFuncs.calcChi0v(molecule)
    descriptors(CHI_1N.id) = RDKFuncs.calcChi1n(molecule)
    descriptors(CHI_1V.id) = RDKFuncs.calcChi1v(molecule)
    descriptors(CHI_2N.id) = RDKFuncs.calcChi2n(molecule)
    descriptors(CHI_2V.id) = RDKFuncs.calcChi2v(molecule)
    descriptors(CHI_3N.id) = RDKFuncs.calcChi3n(molecule)
    descriptors(CHI_3V.id) = RDKFuncs.calcChi3v(molecule)
    descriptors(CHI_4N.id) = RDKFuncs.calcChi4n(molecule)
    descriptors(CHI_4V.id) = RDKFuncs.calcChi4v(molecule)

    descriptors(EXACT_MW.id) = RDKFuncs.calcExactMW(molecule)
    descriptors(FRACTION_CSP3.id) = RDKFuncs.calcFractionCSP3(molecule)

    // 42 mqn values
    var mqnValues = RDKFuncs.calcMQNs(molecule)
    val start = MQN1.id
    (0 until 42).foreach { no => descriptors(start + no) = mqnValues.get(no) }

    descriptors(NUM_ROTATABLE_BONDS.id) = RDKFuncs.calcNumRotatableBonds(molecule)
    descriptors(NUM_HETEROATOMS.id) = RDKFuncs.calcNumHeteroatoms(molecule)
    descriptors(NUM_AMIDE_BONDS.id) = RDKFuncs.calcNumAmideBonds(molecule)
    descriptors(NUM_RINGS.id) = RDKFuncs.calcNumRings(molecule)
    descriptors(NUM_AROMATIC_RINGS.id) = RDKFuncs.calcNumAromaticRings(molecule)
    descriptors(NUM_ALIPHATIC_RINGS.id) = RDKFuncs.calcNumAliphaticRings(molecule)
    descriptors(NUM_SATURATED_RINGS.id) = RDKFuncs.calcNumSaturatedRings(molecule)
    descriptors(NUM_HETEROCYCLES.id) = RDKFuncs.calcNumHeterocycles(molecule)
    descriptors(NUM_AROMATIC_HETEROCYCLES.id) = RDKFuncs.calcNumAromaticHeterocycles(molecule)

    descriptors(NUM_AROMATIC_CARBOCYCLES.id) = RDKFuncs.calcNumAromaticCarbocycles(molecule)
    descriptors(LABUTE_ASA.id) = RDKFuncs.calcLabuteASA(molecule)

    var failed = false
    descriptors.zipWithIndex.foreach {
      case (v, i) => if (v.isNaN) {
        logger.warn(s"Failed to set property ${RdkitDescriptorType(i)}")
        failed = true
      }
    }
    if (failed) None else Some(Vectors.dense(descriptors))
  }
}
