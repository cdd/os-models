package com.cdd.models.utils

import scala.collection.mutable.ArrayBuffer

object VectorUtil {

  implicit class VectorUnzip4[+A](val vector: Vector[A]) {
    def unzip4[A1, A2, A3, A4](implicit asTuple4: A => (A1, A2, A3, A4)): (Vector[A1], Vector[A2], Vector[A3], Vector[A4]) = {
      val b1 = ArrayBuffer[A1]()
      val b2 = ArrayBuffer[A2]()
      val b3 = ArrayBuffer[A3]()
      val b4 = ArrayBuffer[A4]()

      for (abcd <- vector) {
        val (a, b, c, d) = asTuple4(abcd)
        b1 += a
        b2 += b
        b3 += c
        b4 += d
      }
      (b1.toVector, b2.toVector, b3.toVector, b4.toVector)
    }
  }
}
