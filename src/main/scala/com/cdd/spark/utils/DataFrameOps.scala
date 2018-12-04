package com.cdd.spark.utils

import java.nio.file.{Files, Paths}
import java.util.UUID

import com.cdd.models.utils.HasLogging
import org.apache.commons.math3.linear.{BlockRealMatrix, OpenMapRealMatrix, RealMatrix}
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.{DenseVector => OldDenseVector, SparseVector => OldSparseVector, Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scalax.file.Path

/**
  * Created by gjones on 5/20/17.
  */
object DataFrameOps extends HasLogging {

  abstract class DoublePairAccumulator extends Serializable {
    def update(y1: Double, y2: Double): this.type

    def merge(other: DoublePairAccumulator): this.type
  }

  @SerialVersionUID(1000L)
  def dataFramePairAccumulator(df: DataFrame, column1: String, column2: String, accumulator: DoublePairAccumulator): DoublePairAccumulator = {
    val colNo1 = df.schema.fieldIndex(column1)
    val colNo2 = df.schema.fieldIndex(column2)


    df.select(column1, column2).rdd.aggregate(accumulator)(
      seqOp = (counter, row) => {
        counter.update(row.getDouble(0), row.getDouble(1))
      },
      combOp = (counter, other) => {
        counter.merge(other).asInstanceOf[DoublePairAccumulator]
      })
  }

  class MeanSquareDifferenceCounter extends DoublePairAccumulator {
    var count: Int = 0;
    var sumSqrDifference: Double = 0

    def update(y1: Double, y2: Double): this.type = {
      count = count + 1
      sumSqrDifference += (y1 - y2) * (y1 - y2)
      this
    }

    def merge(arg: DoublePairAccumulator): this.type = {
      val other = arg.asInstanceOf[MeanSquareDifferenceCounter]
      count += other.count
      sumSqrDifference += other.sumSqrDifference
      this
    }
  }

  def rmse(df: DataFrame, column1: String, column2: String): Double = {
    var counter = new MeanSquareDifferenceCounter
    counter = dataFramePairAccumulator(df, column1, column2, counter).asInstanceOf[MeanSquareDifferenceCounter]
    Math.sqrt(counter.sumSqrDifference / counter.count)
  }

  /*
  not in use

  def sparseColumnToMllibRowMatix(df: DataFrame, column: String) = {
    val mlSparseVectors = df.select(column).rdd.map(r => r(0).asInstanceOf[org.apache.spark.ml.linalg.SparseVector]).collect
    val mlLibSparseVectors = mlSparseVectors.map { v => org.apache.spark.mllib.linalg.Vectors.sparse(v.size, v.indices, v.values) }
    new RowMatrix(ModelContext.sparkContext.makeRDD(mlLibSparseVectors))
  }
  */

  def filterForEstimator(df: DataFrame, xColumn: String, yColumn: String = "activity_value"): DataFrame = {
    df.select("no", yColumn, xColumn).toDF("no", "label", "features").filter("label is not null and features is not null")
  }

  def singlePartitionToFeatureArray(ds: Dataset[_], labelCol: String = "label", featuresCol: String = "features", forceCoalesce: Boolean = false): Array[(Double, Vector)] = {
    if (!forceCoalesce && ds.rdd.partitions.length != 1) {
      val msg = "Performing singlePartitionToFeatureArray on dataset with multiple partitions, without coalesce set"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    import ds.sparkSession.implicits._
    var df = ds.select(col(labelCol).cast(DoubleType), col(featuresCol))
    if (forceCoalesce) {
      df = df.coalesce(1)
    }
    assume(df.rdd.partitions.length == 1)
    df.map { case Row(label: Double, features: Vector) => (label, features) }
      .mapPartitions { partition =>
        Seq(partition.toArray).toIterator
      }.first()
  }


  /**
    * Converts a column of vectors in a dataframe to a MLLib row matrix
    *
    * @param df
    * @param inputCol
    * @return
    */
  def dataFrameColumnToRowMatrix(df: Dataset[_], inputCol: String): RowMatrix = {
    val input: RDD[OldVector] = df.select(inputCol).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    new RowMatrix(input)
  }

  /**
    * Converts a MLlib RowMatrix to an Apache Math Real Matrix
    *
    * Maintains sparse nature if present
    *
    * @param m
    * @return
    */
  def rowMatrixToRealMatrix(m: RowMatrix): RealMatrix = {
    val rows = m.rows.collect()
    val nRows = rows.length
    var mout: RealMatrix = null

    // assumes that rdd is either all sparse vectors or all dense vectors but not a mix
    rows(0) match {
      case dv: OldDenseVector =>
        mout = new BlockRealMatrix(nRows, dv.size)
        rows.zipWithIndex.foreach { case (v, i) =>
          mout.setRow(i, v.toArray)
        }
      case sv: OldSparseVector =>
        mout = new OpenMapRealMatrix(nRows, sv.size)
        rows.zipWithIndex.foreach { case (v, row) =>
          sv.indices.zip(sv.values).foreach { case (col, vv) => mout.addToEntry(row, col, vv) }
        }
      case _ => throw new IllegalArgumentException("Unknown type of row matrix")
    }
    mout
  }

  /**
    * Converts a dataframe to a CSV file
    *
    * @param df
    * @param csvFile
    */
  def dataFrameToCsv(df: DataFrame, csvFile: String): Unit = {
    // spark will save each partition to a separate CSV file in a directory, so we coalesce to single partition and move
    // that CSV file then remove the directory
    val outDir = UUID.randomUUID().toString
    df.coalesce(1).write.option("header", "true").csv(outDir)

    val csvEntries = Files.newDirectoryStream(Paths.get(outDir), "*.csv").asScala.toList
    if (csvEntries.size != 1) {
      throw new RuntimeException("Not the right number of csv entries")
    }
    Files.move(csvEntries(0), Paths.get(csvFile))
    Path.fromString(outDir).deleteRecursively()
  }
}
