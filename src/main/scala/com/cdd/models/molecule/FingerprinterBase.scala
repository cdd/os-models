package com.cdd.models.molecule

import com.cdd.models.utils.HasLogging
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Created by gjones on 6/21/17.
  */
class FingerprinterBase extends HasLogging{

  def fingerprintToVector(fp: Map[Long, Int]): Vector = {

    val values = Array[Double](fp.size)
    val indices = Array[Int](fp.size)
    fp.zipWithIndex.foreach { case ((index, count), no) =>
        val si = if (index > Int.MaxValue) {
          logger.warn("Truncating fingerprint setting")
          (index - Int.MaxValue).toInt
        } else {
          index.toInt
        }
        indices(no) = si
        values(no) = count.toDouble
    }
    Vectors.sparse(Int.MaxValue, indices, values)
  }

}
