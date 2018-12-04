package com.cdd.models.molecule

import com.cdd.models.utils.HasLogging
import org.RDKit.{MolSanitizeException, RWMol}

object RdkitUtils extends HasLogging {

  def smilesToMol(smiles: String): Option[RWMol] = {
    try {
      val mol = RWMol.MolFromSmiles(smiles)
      Some(mol)
    } catch {
      case ex: MolSanitizeException =>
        logger.warn(s"Failed to sanitize smiles $smiles : ${ex.message()}")
        None
    }
  }

}
