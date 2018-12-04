package com.cdd.models.molecule

import com.cdd.models.utils.Util
import org.openscience.cdk.interfaces.IAtomContainer
import org.scalatest.{FlatSpec, FunSpec, Matchers}

/**
  * Created by gareth on 5/15/17.
  */
class CdkMoleculeReaderSpec extends FunSpec with Matchers {

  describe("Reading a file of molecules") {

    val molecules = CdkMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")

    it("should be able to read a sdf file into an array of molecules") {
      info(s"Read ${molecules.length} molecules")
      molecules.length should be(743)
    }

    describe("Extracting activity data from the file") {
      val activityValues = CdkMoleculeReader.extractProperties("IC50_uM", molecules)
      activityValues.length should be(743)
      activityValues.filter { case Some(s) => s.equals("") case _ => false }
      val noEmptyStrings = activityValues.filter { case Some(s) => s.equals("") case _ => true } .length
      noEmptyStrings should be(0)
      val activityDoubleValues = activityValues.map { case Some(v) => Util.activityContinuousValue(v) case _ => None}
      activityValues.length should be(743)
      val noMissing = activityDoubleValues.filter { _ == None } .length
      noMissing should be(0)
    }
  }
}
