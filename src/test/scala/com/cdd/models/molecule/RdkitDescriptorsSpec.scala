package com.cdd.models.molecule

import com.cdd.models.molecule.RdkitDescriptors.RdkitDescriptorType
import com.cdd.models.utils.{LoadsRdkit, Util}
import org.RDKit.RWMol
import org.scalatest.{FunSpec, Matchers}

/**
  * Created by gjones on 5/31/17.
  */
class RdkitDescriptorsSpec extends FunSpec with Matchers with LoadsRdkit {

  describe("Generating physical properties for a molecule") {

    val molecule = RWMol.MolFromSmiles("Brc1cc(cc(c1O)Br)C=C(c2ccc(cc2)I)C(=O)O")
    val propertyGenerator = new RdkitDescriptors
    val properties = propertyGenerator.physicalProperties(molecule)

    it("should have created physical properties") {
      properties.get.size should be(RdkitDescriptorType.maxId)
    }
  }

  describe("Determining physical properties for a set of molecules") {

    val molecules = RdkitMoleculeReader.readMolecules("data/continuous/Chagas.sdf.gz")

    it("should have loaded some molecules") {
      molecules.length should be(743)
    }

    val properties = RdkitDescriptors.descriptorsForMolecules(molecules)

    it("should have calculated properties for the molecules") {
      info("Hello")
      properties.size should be(743)
      properties(0).get.size should be(RdkitDescriptorType.maxId)
    }
  }
}
