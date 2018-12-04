package scala.com.cdd.bin

import java.nio.file.Files

import scala.collection.JavaConversions._
import org.apache.spark.sql.functions._
import com.cdd.models.utils.{Configuration, Util}
import org.apache.spark.ml.linalg.SparseVector

/**
  * This application counts the unique number of CDK and RDKit FCFPs in the example datasets
  *
  * Created by gjones on 6/19/17.
  */
object FeaturesByDataset extends App {

  Array("data/continuous", "data/discrete").map { Util.getProjectFilePath(_).toPath }.foreach { dir =>
    Files.newDirectoryStream(dir, "*.parquet").foreach { p =>
      val df = Configuration.sparkSession.read.parquet(p.toString)
      val row = df.head()
      val nCdk = row.getAs("fingerprints_CDK_FCFP6").asInstanceOf[SparseVector].size
      val nRdKit = row.getAs("fingerprints_RDKit_FCFP6").asInstanceOf[SparseVector].size
      println(s"dataset ${p.toString} nCDK ${nCdk} nRdKit ${nRdKit}")
    }
  }
}
