package com.cdd.models.universalmetric

import java.io.FileReader

import com.cdd.models.datatable.{DataTable, DataTableColumn, SparseVector}
import com.cdd.models.pipeline._
import com.cdd.models.pipeline.estimator.{SmileNaiveBayesClassifier, SmileSvmClassifier, WekaRandomForestClassifier}
import com.cdd.models.pipeline.tuning.DataSplit
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.CSVReader
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.math.NumberUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LabelIdentifier extends HasLogging {

  def readTrainingDataFile(): Vector[(Vector[String], Double)] = {
    val data = ArrayBuffer[(Vector[String], Double)]()
    val readoutTrainingDataFile = Util.getProjectFilePath("data/vault/readout_fields_training.csv")
    Util.using(new FileReader(readoutTrainingDataFile)) { fh =>
      val reader = new CSVReader(fh)

      val fields = reader.readNext()
      require(fields(2) == "Readout")
      require(fields(3) == "Type")
      require(fields(4) == "Description")
      require(fields(6) == "UseReadout")

      var reading = true
      var nReadouts = 0
      var nNumericReadouts = 0
      var nActivityReadouts = 0
      while (reading) {
        val line = reader.readNext()
        if (line == null) {
          reading = false
        } else {
          nReadouts += 1
          val type_ = line(3)
          if (type_ == "Number") {
            nNumericReadouts += 1
            val readout = line(2)
            val description = line(4)
            var label = line(6).toDouble
            //label = if (label == 0.0) 1.0 else 0.0
            require(label == 0.0 || label == 1.0)
            if (label == 1.0)
              nActivityReadouts += 1
            val tokens = tokenizeAll(Vector(readout, description))
            logger.debug(s"Readout $readout desc $description tokens ${tokens.mkString("|")}")
            data.append((tokens, label))
          }
        }
      }
      logger.info(s"Read $nReadouts readouts, $nNumericReadouts numeric, $nActivityReadouts activity readouts")
      data.toVector
    }
  }

  def tokenizeAll(strs: Vector[String]): Vector[String] = {
    strs.flatMap { s => tokenize(s) }
  }

  private def tokenize(str: String): Vector[String] = {
    var words = str.split("[^\\w\\.]+").toVector.map {
      case "uM" => "um"
      case "mM" => "mm"
      case "nM" => "nm"
      case "pM" => "pm"
      case "Kd" => "kd"
      case "Ki" => "ki"
      case s if isUpperCaseWithDigits(s) => s.toLowerCase()
      case s => s
    }
    words = words.flatMap {
      splitCamelCase
    }.map(_.toLowerCase())
    words = words.flatMap {
      splitWordsWithDot
    }
    words = words.filterNot {
      _ == "."
    }
    if (str.contains("/")) {
      words = words :+ "/"
    }
    words
  }

  private def isUpperCaseWithDigits(str: String): Boolean = {
    str.forall(c => c.isUpper || c.isDigit)
  }

  private def splitWordsWithDot(str: String): Vector[String] = {
    if (NumberUtils.isCreatable(str)) {
      Vector(str)
    } else {
      str.split("\\.").toVector
    }
  }

  private def splitCamelCase(str: String): Vector[String] = {
    // based on code from Apache StringUtils
    if (str.isEmpty) {
      return Vector[String]()
    }
    val c = str.toCharArray
    var list = Vector[String]()
    var tokenStart = 0
    var currentType = Character.getType(c(tokenStart))

    for (pos <- tokenStart + 1 until c.length) {
      val type_ = Character.getType(c(pos))
      if (type_ == 2 && currentType == 1) {
        val newTokenStart = pos - 1
        if (newTokenStart != tokenStart) {
          list = list :+ new String(c, tokenStart, newTokenStart - tokenStart)
          tokenStart = newTokenStart
        }
      }

      currentType = type_
    }

    list :+ new String(c, tokenStart, c.length - tokenStart)
  }
}

@SerialVersionUID(1000L)
class WordCountTransformer(val wordMapping: Map[String, (Int, Int)]) extends Serializable {
  val idfWordMapping: Map[String, (Int, Double)] = calculateIdf()

  private def calculateIdf() = {
    val totalN: Double = wordMapping.size.toDouble
    wordMapping.map { case (word, (index, count)) =>
      val idf = Math.log10(totalN / count.toDouble)
      word -> (index, idf)
    }
  }

  def transform(words: Vector[String], idf: Boolean = false): SparseVector = {
    val (indices, values) = words.foldRight(mutable.Map[Int, Double]().withDefaultValue(0)) { case (word, map) =>
      if (idf) {
        val (index, wt) = idfWordMapping(word)
        map(index) += wt
      } else {
        val (index, _) = wordMapping(word)
        map(index) += 1.0
      }
      map
    }.toArray.sortBy(_._1).unzip
    new SparseVector(indices, values, wordMapping.size)
  }
}

class WordCountVectorizer(val allWords: Vector[Vector[String]]) {
  // Maps words to index and count
  val transformer: WordCountTransformer = vectorize()

  private def vectorize() = {
    val wordCounts =
      allWords.foldRight(mutable.Map[String, Int]().withDefaultValue(0)) { case (words, map) =>
        words.distinct.foreach { w =>
          map(w) += 1
        }
        map
      }.toMap
    val wordMapping = wordCounts.zipWithIndex.map { case ((word, count), index) =>
      word -> (index, count)
    }
    new WordCountTransformer(wordMapping)
  }

  def transform(): Vector[SparseVector] = {
    allWords.map(transformer.transform(_))
  }

  def buildDataTable(labels: Vector[Double]) = {
    require(labels.length == allWords.length)
    val features = transform()
    val words = allWords.map(_.mkString("|"))
    val columns = Vector(DataTableColumn.fromVector("features", features),
      DataTableColumn.fromVector("label", labels),
      DataTableColumn.fromVector("words", words))
    new DataTable(columns)
  }
}

@SerialVersionUID(1000L)
class LabelIdentifierModelAndTransformer(val transformer: WordCountTransformer, val model: PredictionModelWithProbabilities[_])
extends Serializable {

  def predict(readout: String, description: String): Boolean = {
    val tokens = LabelIdentifier.tokenizeAll(Vector(readout, description).filter(StringUtils.isNotBlank))
    val features = transformer.transform(tokens)
    val (category, _) = model.transformRow(features)
    assert(category == 0.0 || category == 1.0)
    category == 1.0
  }

}

object LabelIdentifierModelAndTransformer {
  private val modelFileName = Util.getProjectFilePath("data/vault/readout_fields_model.obj").getAbsolutePath
  lazy val modelAndTransformer:LabelIdentifierModelAndTransformer =
    Util.deserializeObject[LabelIdentifierModelAndTransformer](modelFileName)

  def saveModelAndTransformer(modelAndTransformer: LabelIdentifierModelAndTransformer): Unit = {
    Util.serializeObject(modelFileName, modelAndTransformer)
  }
}

object ReadoutFieldsModel extends App with HasLogging {

  import LabelIdentifier._

  val (allWords, labels) = readTrainingDataFile().unzip
  labels.foreach { l =>
    require(l == .0 || l == 1.0)
  }

  val vectorizer = new WordCountVectorizer(allWords)
  val dataTable = vectorizer.buildDataTable(labels)

  val estimators = Vector(
    new WekaRandomForestClassifier()
      .setFeaturesColumn("features")
      .setMaxDepth(80)
      .setNumTrees(100),
    new SmileSvmClassifier()
      .setKernelType("POLY")
      .setGamma(1.0)
      .setCoef0(1.0)
      .setDegree(2)
      .setC(0.1),
    new SmileSvmClassifier()
      .setKernelType("RBF")
      .setGamma(1.0)
      .setC(1.0),
    new SmileNaiveBayesClassifier()
      .setMethod("Bernoulli")
      .setAlpha(0.5))

  val results = estimators.map { estimator =>
    runEstimator(estimator)
  }.sortBy(-_.metric)
  val best = results(0)
  logger.info(s"best estimator is ${best.estimator}")

  val predictDt = best.dataSplit.getPredictDt()
  predictDt.exportToFile(Util.getProjectFilePath("data/vault/readout_fields_dt.csv").getAbsolutePath)
  val model = best.estimator.fit(dataTable).asInstanceOf[PredictionModelWithProbabilities[_]]
  val testDf = model.transform(dataTable)
  val accuracy = new ClassificationEvaluator().evaluate(testDf)
  logger.info(s"Global accuracy is $accuracy")
  val modelAndTransformer = new LabelIdentifierModelAndTransformer(vectorizer.transformer, model)
  LabelIdentifierModelAndTransformer.saveModelAndTransformer(modelAndTransformer)

  def runEstimator(estimator: Estimator[_, _]): ClassifierValidation = {
    logger.info(s"Running estimator $estimator")
    val evaluator = new ClassificationEvaluator().setMetric("accuracy")
    val dataSplit = new DataSplit().setEvaluator(evaluator).setEstimator(estimator).setNumFolds(3)
    dataSplit.foldAndFit(dataTable)
    val predictDt = dataSplit.getPredictDt()
    val accuracy = evaluator.accuracy(predictDt)
    val rocAuc = evaluator.rocAuc(predictDt)
    val classificationStats = evaluator.statistics(predictDt)
    val precision = classificationStats.precision
    val recall = classificationStats.recall
    logger.info(s"Cross validated accuracy $accuracy rocAuc $rocAuc precision $precision recall $recall")
    new ClassifierValidation("readouts", "readouts", estimator, accuracy, dataSplit,
      evaluator.copy(new ParameterMap()))
  }

}
