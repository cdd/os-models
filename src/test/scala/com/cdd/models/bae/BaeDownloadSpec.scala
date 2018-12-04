package com.cdd.models.bae

import com.cdd.models.molecule.CdkMoleculeReader
import org.scalatest.{FunSpec, Matchers}

class BaeDownloadSpec extends FunSpec with Matchers{

  describe("Downloading data files from BAE") {

    val uniqueId = "pubchemAID:1055"
    val (assayInfo, _) = BaeJsonReader.loadAssayInformationFromServer(uniqueId)
    val (assayCompoundInfo, _) = BaeJsonReader.loadAssayCompoundInformationFromServer(uniqueId)
    val sdFile = "/tmp/base_compounds.sdf.gz"
    BaeJsonReader.downloadSdfile(assayCompoundInfo, sdFile)

    val noCompounds = 193
    it (s"should have downloaded $noCompounds compounds") {
      assayInfo.countCompounds should be(noCompounds)
      assayCompoundInfo.compoundIDList.size() should be(noCompounds)
      CdkMoleculeReader.readMolecules(sdFile).size should be(noCompounds)
    }
  }
}
