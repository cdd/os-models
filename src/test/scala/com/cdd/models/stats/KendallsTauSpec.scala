package com.cdd.models.stats

import com.cdd.models.utils.{CensoredValue, Modifier}
import org.scalatest.{FunSpec, Matchers}

class KendallsTauSpec extends  FunSpec with Matchers {


  describe("Calculating Kendalls Tau B for example data") {
    // example data from Environ Sci Technol. 2007 Jan 1; 41(1): 221–228. supplementary data

    val xValues = Vector(5.0, 4.0, 3.0, 2.0, 6.0)
    val xModifiers = Vector(Modifier.EQUAL,Modifier.LESS_THAN,Modifier.LESS_THAN,Modifier.EQUAL,Modifier.EQUAL )
    val yValues = Vector(1.0, 2.0, 3.0, 2.0, 4.0)
    val yModifiers = Vector(Modifier.EQUAL,Modifier.LESS_THAN,Modifier.EQUAL,Modifier.EQUAL,Modifier.LESS_THAN )

    val tau = KendallsTau.computeTauB(xValues, yValues, Some(xModifiers), Some(yModifiers))

    it ("should calculate the correct Tau") {
      tau should be(-0.338 +- 0.001)
    }
  }
}
