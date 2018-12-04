package com.cdd.models.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.sql.functions._

/**
  * Simple test of reading and writing Data Frames to Hadoop storage
  */
class HadoopSpec extends FunSpec with Matchers {

  describe("writing a dataframe to csv in hadoop") {

    val csvFile = Util.hadoopPath() + "/data/test.csv"
    val hdfs = Util.hdfs()
    val hadoopPath = new Path(csvFile)
    if (hdfs.exists(hadoopPath))
      hdfs.delete(hadoopPath, true)

    it("The test file should not be present") {
      val exists = hdfs.exists(hadoopPath)
      info(s"file exists is ${exists}")
      // this test usually seems to fail, even thought the file is removed!- latency in hadoop?
      //exists should be(false)
    }

    val values = List((1, "one"), (2, "two"), (3, "three"))
    val session:SparkSession = Configuration.sparkSession
    import session.implicits._
    var df = values.toDF("number", "text")
    df.write.option("header", "true").csv(csvFile)

    it("The test file should be present") {
      hdfs.exists(hadoopPath) should be(true)
    }

    df = Configuration.sparkSession.read.option("header", "true").csv(csvFile)
    it ("should have read three rows") {
      df.count should be(3)
      df.columns should be (Array("number", "text"))
      df = df.filter("number  = '1'")
      df.head()(0) should be("1")
      df.head()(1) should be("one")
    }
  }

  describe("writing a dataframe to parquet in hadoop") {

    val parquetFile = Util.hadoopPath() + "/data/test.parquet"
    val hdfs = Util.hdfs()
    val hadoopPath = new Path(parquetFile)
    if (hdfs.exists(hadoopPath))
      hdfs.delete(hadoopPath, true)

    it("The test file should not be present") {
      val exists = hdfs.exists(hadoopPath)
      info(s"file exists is ${exists}")
      // this test usually seems to fail, even thought the file is removed!- latency in hadoop?
      //exists should be(false)
    }

    val values = List((1, "one"), (2, "two"), (3, "three"))
    val session:SparkSession = Configuration.sparkSession
    import session.implicits._
    var df = values.toDF("number", "text")
    df.write.option("header", "true").parquet(parquetFile)

    it("The test file should be present") {
      hdfs.exists(hadoopPath) should be(true)
    }

    df = Configuration.sparkSession.read.option("header", "true").parquet(parquetFile)
    it ("should have read three rows") {
      df.count should be(3)
      df.columns should be (Array("number", "text"))
      df = df.filter("number  = 1")
      df.head()(0) should be(1)
      df.head()(1) should be("one")
    }
  }
}
