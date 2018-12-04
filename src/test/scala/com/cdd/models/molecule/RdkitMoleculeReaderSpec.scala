package com.cdd.models.molecule

import com.cdd.models.utils.Util
import org.RDKit.ROMol
import org.scalatest.{FunSpec, Ignore, Matchers}

/**
  * Created by gjones on 5/23/17.
  */
class RdkitMoleculeReaderSpec extends FunSpec with Matchers {

  describe("Reading a file of molecules") {

    val molecules = RdkitMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")

    it("should be able to read a sdf file into an array of molecules") {
      info(s"Read ${molecules.length} molecules")
      molecules.length should be(743)
    }


    describe("Extracting activity data from the file") {
      val activityValues = RdkitMoleculeReader.extractProperties("IC50_uM", molecules)
      val noEmptyStrings = activityValues.filter { case Some(s) => s.equals("") case _ => true }.length
      val activityDoubleValues = activityValues.map { case Some(v) => Util.activityContinuousValue(v) case _ => None }
      val noMissing = activityDoubleValues.filter {
        _ == None
      }.length

      it("should have read the file correctly") {
        noEmptyStrings should be(2)
        activityValues.length should be(743)
        activityDoubleValues.length should be(743)
        noMissing should be(2)
      }
    }

  }
}
