package com.cdd.models.molecule

import com.cdd.models.molecule.CdkDescriptors.CdkDescriptorType
import org.openscience.cdk.qsar.{DescriptorEngine, IMolecularDescriptor}
import org.openscience.cdk.silent.SilentChemObjectBuilder
import org.openscience.cdk.smiles.SmilesParser
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 5/31/17.
  */
class CdkDescriptorsSpec extends FunSpec with Matchers {

  describe("Generating physical properties for a molecule") {

    val sp = new SmilesParser(SilentChemObjectBuilder.getInstance())
    val molecule = sp.parseSmiles("Brc1cc(cc(c1O)Br)C=C(c2ccc(cc2)I)C(=O)O")

    val propertyGenerator = new CdkDescriptors
    val properties = propertyGenerator.descriptors(molecule)


    it("should have created physical properties") {
      properties.get.size should be(CdkDescriptorType.maxId)
    }
  }


  describe("Determining physical properties for a set of molecules") {

    val molecules = CdkMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")

    it("should have loaded some molecules") {
      molecules.length should be(743)
    }

    val properties = CdkDescriptors.descriptorsForMolecules(molecules)
    val cnt = properties.filter { _ != None } .length

    it("should have calculated properties for the molecules") {
      properties.size should be(743)
      properties(0).get.size should be(CdkDescriptorType.maxId)
      // two structures fail to get BCUT properties
      cnt should be(741)
    }
  }
}
