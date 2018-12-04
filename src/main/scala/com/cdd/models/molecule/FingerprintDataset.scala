package com.cdd.models.molecule


import org.apache.spark.ml.linalg.{Vector, Vectors}


object FingerprintTransform extends Enumeration {
  type FingerprintTransform = Value
  val Fold, Sparse = Value
}

/**
  * Created by gareth on 5/15/17.
  */

class FingerprintDataset(fingerprintHashLookup: List[Option[Map[Long, Int]]]) {

  // the set of all unique hashes in the set of fingerprints
  val uniqueHashesSet: Set[Long] = (fingerprintHashLookup.filter {
    _ != None
  }.flatMap { fp => fp.get.keys }).toSet
  // all unique hashes mapped to a sequential index
  val uniqueHashesIndex: Map[Long, Int] = uniqueHashesSet.toList.zipWithIndex.map { case (hash, index) => hash -> index }.toMap

  def toFolded(foldSize: Int): List[Option[Vector]] = {
    fingerprintHashLookup.map { case Some(fp) => Some(foldFingerprint(foldSize, fp)) case _ => None }
  }

  def toSparse(): List[Option[Vector]] = {
    fingerprintHashLookup.map { case Some(fp) => Some(fingerprintToSparseVector(fp)) case _ => None }
  }

  private def foldFingerprint(foldSize: Int, fingerprint: Map[Long, Int]) = {
    val foldedValues = Array.fill(foldSize) {
      .0
    }
    fingerprint.foreach { case (hash, count) =>
      val index = uniqueHashesIndex(hash)
      val fold = index % foldSize;
      foldedValues(fold) += count
    }
    Vectors.dense(foldedValues)
  }

  private def fingerprintToSparseVector(fingerprint: Map[Long, Int]) = {
    val sparseValues = fingerprint.map { case (hash, count) => (uniqueHashesIndex(hash), count.toDouble) }.toList
    Vectors.sparse(uniqueHashesIndex.size, sparseValues)
  }

  def toVectors(transform: FingerprintTransform.Value, foldSize: Int = 1024): List[Option[Vector]] = {
    transform match {
      case FingerprintTransform.Fold => toFolded(foldSize)
      case FingerprintTransform.Sparse => toSparse()
    }
  }
}
