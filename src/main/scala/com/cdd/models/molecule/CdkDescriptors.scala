package com.cdd.models.molecule

import com.cdd.models.utils.HasLogging
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.qsar.result.{DoubleArrayResult, DoubleResult, IntegerArrayResult, IntegerResult}
import org.openscience.cdk.qsar.{DescriptorEngine, DescriptorValue, IMolecularDescriptor}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{DynamicVariable, Failure, Success}

object CdkDescriptors extends HasLogging {

  // These are 2D properties- there is a slew of additional properties if we have 3D molecule- e.g. see first compound in discrete/data_chagas.sdf.gz

  object CdkDescriptorType extends Enumeration {
    type CdkPhysicalPropertyType = Value

    val nSmallRings, nAromRings, nRingBlocks, nAromBlocks, nRings3, nRings4, nRings5, nRings6, nRings7, nRings8, nRings9,
    tpsaEfficiency, Zagreb, XLogP, WPATH, WPOL,
    MW, VAdjMat, TopoPSA, LipinskiFailures, nRotB, topoShape, PetitjeanNumber,
    MDEC_11, MDEC_12, MDEC_13, MDEC_14, MDEC_22, MDEC_23, MDEC_24, MDEC_33, MDEC_34, MDEC_44, MDEO_11, MDEO_12, MDEO_22, MDEN_11, MDEN_12, MDEN_13, MDEN_22, MDEN_23, MDEN_33,
    MLogP, nAtomLAC, nAtomP, nAtomLC,

    khs_sLi, khs_ssBe, khs_ssssBe, khs_ssBH, khs_sssB, khs_ssssB, khs_sCH3, khs_dCH2, khs_ssCH2, khs_tCH, khs_dsCH, khs_aaCH,
    khs_sssCH, khs_ddC, khs_tsC, khs_dssC, khs_aasC, khs_aaaC, khs_ssssC, khs_sNH3, khs_sNH2, khs_ssNH2, khs_dNH, khs_ssNH,
    khs_aaNH, khs_tN, khs_sssNH, khs_dsN, khs_aaN, khs_sssN, khs_ddsN, khs_aasN, khs_ssssN, khs_sOH, khs_dO, khs_ssO, khs_aaO,
    khs_sF, khs_sSiH3, khs_ssSiH2, khs_sssSiH, khs_ssssSi, khs_sPH2, khs_ssPH, khs_sssP, khs_dsssP, khs_sssssP, khs_sSH, khs_dS,
    khs_ssS, khs_aaS, khs_dssS, khs_ddssS, khs_sCl, khs_sGeH3, khs_ssGeH2, khs_sssGeH, khs_ssssGe, khs_sAsH2, khs_ssAsH, khs_sssAs,
    khs_sssdAs, khs_sssssAs, khs_sSeH, khs_dSe, khs_ssSe, khs_aaSe, khs_dssSe, khs_ddssSe, khs_sBr, khs_sSnH3, khs_ssSnH2,
    khs_sssSnH, khs_ssssSn, khs_sI, khs_sPbH3, khs_ssPbH2, khs_sssPbH, khs_ssssPb,

    Kier1, Kier2, Kier3, HybRatio, nHBDon, nHBAcc, fragC, FMF, ECCEN,

    C1SP1, C2SP1, C1SP2, C2SP2,
    C3SP2, C1SP3, C2SP3, C3SP3, C4SP3, bpol, nB, BCUTw_1l, BCUTw_1h, BCUTc_1l, BCUTc_1h, BCUTp_1l, BCUTp_1h, nBase,
    ATSp1, ATSp2, ATSp3, ATSp4, ATSp5, ATSm1, ATSm2, ATSm3, ATSm4, ATSm5, ATSc1, ATSc2, ATSc3, ATSc4, ATSc5,
    nAtom, nAromBond, naAromAtom, apol, ALogP, ALogp2, AMR, nAcid = Value

  }

  // geomShape is not defined (presumably because molecules are 2D)
  // others identified by Peter Lind as taking too Long: WTPT_* (WeightedPathDescriptor), SP_*, VP_* (ChiPathDescriptor),
  // SPC-* VPC-* (ChiPathClusterDescriptor)
  // SC-*, VC-* (ChiClusterDescriptor) SCH-*, VCH-* (ChiChainDescriptor)
  // The AminoAcidCountDescriptor n*

  // I've confirmed that with the exception of gemoShape, none of these properties are calculated by the listed descriptor classes:
  private val excludedDescriptors = Set("geomShape",
    "WTPT_1", "WTPT_2", "WTPT_3", "WTPT_4", "WTPT_5", "SP_0", "SP_1", "SP_2", "SP_3", "SP_4", "SP_5", "SP_6", "SP_7",
    "VP_0", "VP_1", "VP_2", "VP_3", "VP_4", "VP_5", "VP_6", "VP_7",
    "SPC_4", "SPC_5", "SPC_6", "VPC_4", "VPC_5", "VPC_6",
    "SC_3", "SC_4", "SC_5", "SC_6", "VC_3", "VC_4", "VC_5", "VC_6",
    "SCH_3", "SCH_4", "SCH_5", "SCH_6", "SCH_7", "VCH_3", "VCH_4", "VCH_5", "VCH_6", "VCH_7",
    "nV", "nA", "nR", "nN", "nD", "nC", "nF", "nQ", "nE", "nG", "nH", "nI", "nP", "nL", "nK", "nM", "nS", "nT", "nY", "nW"
  )

  // These are all the current 2D descriptor classes, with the exception of ChiPathDescriptor, ChiChainDescriptor,
  // ChiPathClusterDescriptor, WeightedPathDescriptor and ChiClusterDescriptor which take too long to calculate
  // and AminoAcidCountDescriptor which doesn't make sense for most small molecules

  val descriptorClasses = List("KierHallSmartsDescriptor", "BCUTDescriptor", "AutocorrelationDescriptorPolarizability",
    "FMFDescriptor", "RuleOfFiveDescriptor", "MDEDescriptor", "XLogPDescriptor", "KappaShapeIndicesDescriptor",
    "AutocorrelationDescriptorCharge", "FractionalPSADescriptor", "RotatableBondsCountDescriptor",
    "AutocorrelationDescriptorMass", "PetitjeanShapeIndexDescriptor", "PetitjeanNumberDescriptor", "SmallRingDescriptor",
    "LongestAliphaticChainDescriptor", "WienerNumbersDescriptor", "TPSADescriptor", "HybridizationRatioDescriptor",
    "EccentricConnectivityIndexDescriptor", "SmallRingDescriptor", "TPSADescriptor", "MannholdLogPDescriptor",
    "HBondAcceptorCountDescriptor", "HBondDonorCountDescriptor", "IPMolecularLearningDescriptor", "LargestPiSystemDescriptor",
    "AromaticAtomsCountDescriptor", "GravitationalIndexDescriptor", "AromaticBondsCountDescriptor",
    "MomentOfInertiaDescriptor", "WHIMDescriptor", "CarbonTypesDescriptor", "LargestChainDescriptor", "ZagrebIndexDescriptor",
    "APolDescriptor", "LengthOverBreadthDescriptor", "BPolDescriptor", "VABCDescriptor", "BondCountDescriptor",
    "AtomCountDescriptor", "FragmentComplexityDescriptor", "VAdjMaDescriptor", "WeightDescriptor", "ALOGPDescriptor",
    "AcidicGroupCountDescriptor", "BasicGroupCountDescriptor")
    .map {
      "org.openscience.cdk.qsar.descriptors.molecular." + _
    }

  val descriptorEngine = new DynamicVariable[DescriptorEngine](new DescriptorEngine(descriptorClasses.asJava, null))

  def descriptorsForMolecules(molecules: List[Option[IAtomContainer]]): List[Option[Vector]] = {
    val propertyCalculator = new CdkDescriptors
    molecules.map {
      case Some(molecule) => propertyCalculator.descriptors(molecule)
      case None => None
    }
  }

  /*
  def excludeClasses: Unit = {
    descriptorClasses.filter {
      className =>
        val desciptor = Class.forName(className).newInstance().asInstanceOf[IMolecularDescriptor]
        val skipDescriptor = desciptor.getDescriptorNames.forall { name =>
          val propertyTypeName = name.replace('.', '_').replace('-', '_')
          excludedDescriptors.contains(name) || excludedDescriptors.contains(propertyTypeName)
        }
        if (skipDescriptor) {
          logger.info(s"Excluding descriptor class $className")
        }
      !skipDescriptor
    }
  }
  */
}

import com.cdd.models.molecule.CdkDescriptors.{CdkDescriptorType, descriptorEngine, excludedDescriptors}

class CdkDescriptors() extends HasLogging {
  var calculationNo = 0

  def descriptors(molecule: IAtomContainer): Option[Vector] = this.synchronized {
    // CDK descriptor calculations are not scalable to large molecules- e.g fails to compute on molecule of 488 atoms
    // in any reasonable time, so allow up to 1 minute to calculate descriptors
    // Note calculation is not thread-safe, even though descriptor engine is per thread
    val molDescriptors = Array.fill[Double](CdkDescriptorType.maxId)(Double.NaN)
    val futureResult: Future[Unit] = Future {
      descriptorEngine.value.process(molecule)
    }
    try {
      val result = Await.ready(futureResult, 60.seconds).value.get
      result match {
        case Success(_) =>
        case Failure(ex) =>
          logger.warn("Failed to determine descriptors for molecule", ex)
          return None
      }
    } catch {
      case _: TimeoutException =>
        logger.error(s"Timeout while determining descriptors for molecule with ${molecule.getAtomCount} atoms")
        return None
    }

    calculationNo += 1
    if (calculationNo % 100 == 0)
      logger.info(s"Determining properties for molecule, calculation ${calculationNo}")
    val propertyGroups = molecule.getProperties.values().asScala.filter { v => v.isInstanceOf[DescriptorValue] }.map { v => v.asInstanceOf[DescriptorValue] }
    propertyGroups.foreach { v =>
      val names = v.getNames.array
      v.getValue match {
        case dv: DoubleResult => {
          addProperty(molDescriptors, names(0), dv.doubleValue())
        }
        case iv: IntegerResult => {
          addProperty(molDescriptors, names(0), iv.intValue())
        }
        case da: DoubleArrayResult => {
          if (da.length() > names.length) {
            logger.warn("Insufficent names for double array result")
          } else {
            (0 until da.length()).foreach { i =>
              addProperty(molDescriptors, names(i), da.get(i))
            }
          }
        }
        case ia: IntegerArrayResult => {
          if (ia.length() > names.length) {
            logger.warn("Insufficent names for integer array result")
          } else {
            (0 until ia.length()).foreach { i =>
              addProperty(molDescriptors, names(i), ia.get(i))
            }
          }
        }
        case _ => logger.warn(s"Unknown property type ${v.getValue.getClass}")
      }
    }

    var failed = false
    molDescriptors.zipWithIndex.foreach {
      case (v, i) => if (v.isNaN) {
        logger.warn(s"Failed to set property ${CdkDescriptorType(i)}")
        failed = true
      }
    }
    if (failed) None else Some(Vectors.dense(molDescriptors))
  }

  private def addProperty(molProperties: Array[Double], name: String, value: Double)

  = {
    val propertyTypeName = name.replace('.', '_').replace('-', '_')
    if (!excludedDescriptors.contains(name) && !excludedDescriptors.contains(propertyTypeName)) {
      try {
        val propertyType = CdkDescriptorType.withName(propertyTypeName)
        molProperties(propertyType.id) = value
      } catch {
        case _: NoSuchElementException => logger.warn(s"Unknown physical property ${propertyTypeName}")
      }
    } else {
      logger.debug(s"excluding descriptor $name")
      // currently should only ever skip geomShape
      require(name == "geomShape")
    }
  }
}
