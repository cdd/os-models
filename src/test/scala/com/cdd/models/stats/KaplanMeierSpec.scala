package com.cdd.models.stats

import com.cdd.models.utils.{CensoredValue, Modifier}
import org.scalatest.{FunSpec, Matchers}

class KaplanMeierSpec extends FunSpec with Matchers {
  describe("Computing KM estimator correctly for example data") {
    // example data from GRAPHICAL ASSESSMENT OF CENSORED DATA
    val data = Vector(new CensoredValue(6, Modifier.EQUAL), new CensoredValue(3, Modifier.EQUAL),
      new CensoredValue(5, Modifier.LESS_THAN),
      new CensoredValue(10, Modifier.LESS_THAN), new CensoredValue(6, Modifier.EQUAL),
      new CensoredValue(12, Modifier.EQUAL),
      new CensoredValue(5, Modifier.EQUAL), new CensoredValue(5, Modifier.LESS_THAN))
    val km = new KaplanMeier(data)

    it("should have 4 entries in the table") {
      km.table.length should be(4)
    }

    var entry = km.table(0)
    it("should have correct values for entry 0") {
      entry.total should be(8)
      entry.value should be(12.0)
      entry.noUncensored should be(1)
      entry.f should be(1.0)
    }

    entry = km.table(1)
    it("should have correct values for entry 1") {
      entry.total should be(6)
      entry.value should be(6.0)
      entry.noUncensored should be(2)
      entry.f should be(0.875 +- 0.001)
    }

    entry = km.table(2)
    it("should have correct values for entry 2") {
      entry.total should be(4)
      entry.value should be(5.0)
      entry.noUncensored should be(1)
      entry.f should be(0.5833 +- 0.001)
    }


    entry = km.table(3)
    it("should have correct values for entry 3") {
      entry.total should be(1)
      entry.value should be(3.0)
      entry.noUncensored should be(1)
      entry.f should be(0.4375 +- 0.001)
    }

    it("should have a mean of 5.292") {
      km.mean should be(5.292 +- 0.001)
    }

    it("should have a variance of 9.236") {
      km.variance should be(9.236 +- 0.001)
    }

  }
}
