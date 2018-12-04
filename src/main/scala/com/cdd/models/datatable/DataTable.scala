package com.cdd.models.datatable

import java.io._
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.cdd.models.pipeline.tuning.IdenticalSplitLabels
import com.cdd.models.utils.{HasLogging, Util}
import com.opencsv.{CSVReader, CSVWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseVector => MlDenseVector, SparseVector => MlSparseVector, Vector => MlVector}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.Serializable


object DataTable extends HasLogging {

  def fromSpark(ds: DataFrame, forceCoalesce: Boolean = false): DataTable = {

    if (!forceCoalesce && ds.rdd.partitions.length != 1) {
      val msg = "Performing singlePartitionToFeatureArray on dataset with multiple partitions, without coalesce set"
      logger.error(msg)
      throw new RuntimeException(msg)
    }

    var df = ds
    if (forceCoalesce) {
      df = df.coalesce(1)
    }
    assume(df.rdd.partitions.length == 1)
    new DataTable(df.columns.map {
      extractSparkColumnValues(df, _)
    }.toVector)
  }

  private def extractSparkColumnValues(df: DataFrame, column: String): DataTableColumn[_] = {
    val intMap = (o: Object) => o.asInstanceOf[Integer].toInt
    val doubleMap = (o: Object) => o.asInstanceOf[Double].toDouble
    df.schema(column).dataType match {
      case StringType => new DataTableColumn[String](column, getValues[String](df.select(col(column)), classOf[String], None), classOf[String])
      case DoubleType => new DataTableColumn[Double](column, getValues[Double](df.select(col(column)), classOf[Double], Some(doubleMap)), classOf[Double])
      case IntegerType => new DataTableColumn[Int](column, getValues[Int](df.select(col(column)), classOf[Int], Some(intMap)), classOf[Int])
      case VectorType => createVectorColumn(df.select(column))
      case _ => throw new IllegalStateException("Unsupported column type")
    }
  }

  private def getValues[T](df: DataFrame, clazz: Class[T], mapFunction: Option[(Object) => T]): Vector[Option[T]] = {
    assume(df.rdd.partitions.length == 1)
    df.collect().map {
      case Row(v) =>
        v match {
          case null => None
          case v =>
            val raw = if (mapFunction == None) clazz.cast(v) else mapFunction.get(v.asInstanceOf[Object])
            Some(raw)
        }
    }.toVector
  }

  private def createVectorColumn(df: DataFrame) = {
    assume(df.rdd.partitions.length == 1)
    val values = df.collect().map {
      case Row(v: MlVector) => (v)
      case Row(null) => null
    }
      .map {
        case d: MlDenseVector => Some(Vectors.Dense(d.toArray))
        case s: MlSparseVector => Some(Vectors.Sparse(s.indices, s.values, s.size))
        case null => None
      }.toVector
    new DataTableColumn[VectorBase](df.columns(0), values)
  }

  def importFromCsv(in: Reader, separator: Char = ','): DataTable = {
    val reader = new CSVReader(in, separator)
    val pattern = """^(.*) \[([a-zA-Z]+)\]$""".r
    var hasTypes = true

    val headers: List[(String, Class[_])] = reader.readNext().toSeq.map { v =>
      val m = pattern.findFirstMatchIn(v)
      if (m.isEmpty) hasTypes = false
      val title = if (m.isDefined) m.get.group(1) else v
      val typeName = m match {
        case None => classOf[String]
        case Some(mat) => mat.group(2) match {
          case "String" => classOf[String]
          case "Int" => classOf[Int]
          case "Double" => classOf[Double]
          case "SparseVector" => classOf[SparseVector]
          case "DenseVector" => classOf[DenseVector]
          case _ => throw new IllegalArgumentException("Unknown data type")
        }
      }
      (title, typeName)
    }
      .toList

    val (titles, types) = headers.unzip

    val buffers = types.map { t =>
      (t, new ListBuffer[Option[Any]])
    }

    var reading = true
    while (reading) {
      val nextLine = reader.readNext()
      if (nextLine == null) {
        reading = false
      }
      else {
        nextLine.zip(buffers).foreach { case (v, (clazz, buffer)) =>
          if (v == null || v.isEmpty) {
            buffer.append(None)
          }
          else {
            clazz match {
              case c if c == classOf[String] =>
                buffer.append(Some(v))
              case c if c == classOf[Int] =>
                buffer.append(Some(v.toInt))
              case c if c == classOf[Double] =>
                buffer.append(Some(v.toDouble))
              case c if c == classOf[SparseVector] =>
                val strs = v.split(':')
                require(strs.length == 3)
                val (sizeStr, indexStr, dataStr) = (strs(0), strs(1), strs(2))
                buffer.append(Some(Vectors.Sparse(indexStr.split('|').map(_.toInt), dataStr.split('|').map(_.toDouble), sizeStr.toInt)))
              case c if c == classOf[DenseVector] =>
                buffer.append(Some(Vectors.Dense(v.split('|').map(_.toDouble))))
              case _ => throw new IllegalArgumentException(s"Unknown data type ${clazz.getName}")
            }
          }
        }
      }
    }

    val columns = buffers.zip(titles).map { case ((clazz, buffer), title) =>
      val col = new DataTableColumn[Any](title, buffer.toVector)
      if (hasTypes) {
        col
      } else {
        require(col.clazz == classOf[String])
        val intCol = DataTableColumn.stringToIntColumn(col.asInstanceOf[DataTableColumn[String]])
        val doubleCol = if (intCol.isDefined) None else DataTableColumn.stringToDoubleColumn(col.asInstanceOf[DataTableColumn[String]])
        if (intCol.isDefined)
          intCol.get
        else if (doubleCol.isDefined)
          doubleCol.get
        else
          col
      }
    }.toVector
    new DataTable(columns)
  }


  def loadFile(fileName: String, separator: Char = ','): DataTable = {
    var dt: Option[DataTable] = None
    fileName.toLowerCase match {
      case f if f.endsWith(".csv") =>
        Util.using(new BufferedReader(new FileReader(fileName))) { in =>
          dt = Some(DataTable.importFromCsv(in, separator))
        }
      case f if f.endsWith(".csv.gz") =>
        Util.using(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileName)))) { in =>
          dt = Some(DataTable.importFromCsv(in, separator))
        }
      case _ => throw new IllegalArgumentException(s"Unknown file type for file ${fileName}")
    }
    dt.get
  }

  def loadProjectFile(fileName: String, separator: Char = ','): DataTable = {
    loadFile(Util.getProjectFilePath(fileName).getAbsolutePath, separator)
  }


}

@SerialVersionUID(1000L)
class DataTable(val columns: Vector[DataTableColumn[_]]) extends Serializable {

  require(columns.nonEmpty)
  val length = columns(0).length
  columns.foreach { c => require(c.length == length) }

  def addColumn(column: DataTableColumn[_]): DataTable = {
    require(column.length == columns(0).length)
    require(!hasColumnWithName(column.title))
    new DataTable(columns :+ column)
  }

  def addConstantColumn[A](title: String, value: A) = {
    val values = Vector.fill(length)(Some(value))
    val column = new DataTableColumn[A](title, values)
    addColumn(column)
  }

  def extractIndices(indices: Seq[Int]): DataTable = {
    new DataTable(columns.map(_.extractIndices(indices)))
  }

  def removeIndices(indices: Set[Int]): DataTable = {
    new DataTable(columns.map(_.removeIndices(indices)))
  }

  def exportToFile(fileName: String, writeHeaders: Boolean = true, append: Boolean = false, writeTypes: Boolean = true): Unit = {
    fileName.toLowerCase match {
      case f if f.endsWith(".csv") =>
        Util.using(new BufferedWriter(new FileWriter(fileName, append))) { out =>
          exportToCsv(out, writeHeaders, writeTypes)
        }
      case f if f.endsWith(".csv.gz") =>
        require(!append)
        Util.using(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fileName)))) { out =>
          exportToCsv(out, writeHeaders, writeTypes)
        }
      case _ => throw new IllegalArgumentException(s"Unknown file type for file ${fileName}")
    }
  }

  def exportToCsv(out: Writer, writeHeaders: Boolean = true, writeTypes: Boolean = true): Unit = {
    val writer = new CSVWriter(out)
    if (writeHeaders) {
      writer.writeNext(columns.map {
        if (writeTypes)
          _.stringHeader()
        else
          _.title

      }.toArray)
    }
    0.until(length).foreach { index =>
      writer.writeNext(columns.map {
        _.valueToString(index)
      }.toArray)
    }
  }

  def shuffle(seed: Long = 1234L): DataTable = {
    val rnd = new Random(seed)
    val indices = rnd.shuffle[Int, IndexedSeq](0.until(length))
    val newColumns = columns.map {
      _.extractIndices(indices)
    }
    val newTable = new DataTable(newColumns)
    assert(length == newTable.length)
    newTable
  }

  def sort(columnName: String, reverse:Boolean=false): DataTable = {
    val column = this.column(columnName)
    val values = column.values
    var indices = Vector.range(0, length).sortWith { case (ia, ib) =>
      (values(ia), values(ib)) match {
        case (Some(a), Some(b)) =>
          column.clazz match {
            case c if c == classOf[Double] || c == classOf[java.lang.Double] =>
              a.asInstanceOf[Double] > b.asInstanceOf[Double]
            case c if c == classOf[Int] || c == classOf[java.lang.Integer] =>
              a.asInstanceOf[Int] > b.asInstanceOf[Int]
            case c if c == classOf[String] || c == classOf[java.lang.String] =>
              a.asInstanceOf[String] > b.asInstanceOf[String]
            case _ => throw new IllegalArgumentException
          }
        case (None, Some(_)) => false
        case (Some(_), None) => true
        case (None, None) => false
      }
    }
    if (reverse)
      indices = indices.reverse
    val newColumns = columns.map {
      _.extractIndices(indices)
    }
    val newTable = new DataTable(newColumns)
    assert(length == newTable.length)
    newTable
  }

  def slice(start: Int = 0, end: Int = length): DataTable = {
    val newColumns = columns.map {
      _.slice(start, end)
    }
    new DataTable(newColumns)
  }

  def testTrainSplit(testProportion: Double = 0.2): (DataTable, DataTable) = {
    require(testProportion > 0 && testProportion < 1.0)
    val split = (length * testProportion + 0.5).toInt
    val test = slice(end = split)
    val train = slice(start = split)
    assert(test.length + train.length == length)
    (test, train)
  }

  def trainValidateFold(foldNo: Int, nFolds: Int = 3): (DataTable, DataTable) = {
    require(nFolds <= length)
    require(foldNo < nFolds)
    val validate = extractIndices(0.until(length).filter(_ % nFolds == foldNo))
    val train = extractIndices(0.until(length).filter(_ % nFolds != foldNo))
    assert(validate.length + train.length == length)
    (train, validate)
  }

  def column(columnNo: Int): DataTableColumn[_] = {
    columns(columnNo)
  }

  def column(columnName: String): DataTableColumn[_] = {
    val col = columns.find {
      _.title == columnName
    }
    col match {
      case Some(column) => column
      case _ => throw new IllegalArgumentException(s"Unable to find column titled ${columnName} in data table")
    }
  }

  def removeColumns(columnNames: String*): DataTable = {
    val cols = columns.filter { c =>
      !columnNames.contains(c.title)
    }
    new DataTable(cols)
  }

  def hasColumnWithName(columnName: String): Boolean = {
    val col = columns.find {
      _.title == columnName
    }
    col.isDefined
  }

  def select(columnNames: String*): DataTable = {
    val columns = columnNames.map {
      column
    }.toVector
    new DataTable(columns)
  }

  def selectAs(cols: Tuple2[String, String]*): DataTable = {
    val columns = cols.map {
      case (columnName, newColumnName) => {
        new DataTableColumn[Any](newColumnName, column(columnName).values)
      }
    }.toVector
    new DataTable(columns)
  }

  def rename(cols: Tuple2[String, String]*): DataTable = {
    val colMap = cols.toMap
    val mappedColumns = mutable.Set[String]()
    val newColumns = columns.map { column =>
      if (colMap.contains(column.title)) {
        mappedColumns.add(column.title)
        new DataTableColumn[Any](colMap(column.title), column.values)
      } else {
        column
      }
    }
    require(mappedColumns.toSet == colMap.keys.toSet)

    new DataTable(newColumns)
  }

  def filterByColumnCriteria[A](columnName: String, filterFn: (A) => Boolean): DataTable = {
    val col = column(columnName).asInstanceOf[DataTableColumn[A]]
    val matchingIndices = col.matchingIndices(filterFn, notMatching = true)
    removeIndices(matchingIndices)
  }

  def filterNull(): DataTable = {
    val nullRowIndices = mutable.Set[Int]()
    columns.foreach { c => nullRowIndices ++= c.nullValuesIndices() }
    removeIndices(nullRowIndices.toSet)
  }

  def append(other: DataTable): DataTable = {
    require(other.columns.length == columns.length)
    new DataTable(columns.zip(other.columns).map { case (col1, col2) => col1.asInstanceOf[DataTableColumn[Any]].append(col2.asInstanceOf[DataTableColumn[Any]]) })
  }

  def filterByColumnValues(columnName: String, values: Vector[_]): DataTable = {
    val indices = column(columnName).asInstanceOf[DataTableColumn[Any]].matchingIndices(values)
    new DataTable(columns.map(_.extractIndices(indices)))
  }

  def split(splitSize: Int): Vector[DataTable] = {
    if (splitSize >= length) {
      return Vector(this)
    }
    val ranges = Vector.range(0, length, splitSize) :+ length
    val dataTables = ranges.sliding(2).map { case (Vector(s, e)) =>
      slice(s, e)
    }.toVector
    assert(dataTables.map(_.length).sum == length)
    dataTables
  }

  def continuousColumnToDiscrete(columnName: String, threshold: Double, greaterBetter: Boolean = true, newColumnName: String = "active"): DataTable = {
    val continuousValues = column(columnName).values.asInstanceOf[Vector[Option[Double]]]
    val discreteValues = continuousValues.map {
      case Some(d) =>
        val active = if (greaterBetter) d >= threshold else d <= threshold
        if (active) Some(1.0) else Some(0.0)
      case None => None
    }
    discreteValues.flatten.toSet.size match {
      case 1 => throw new IdenticalSplitLabels("continuousColumnToDiscrete: only one split label!")
      case s => require(s == 2)
    }
    new DataTable(columns :+ new DataTableColumn[Double](newColumnName, discreteValues))
  }

  def classifierPredictionSplit(columnName: String): (DataTable, DataTable) = {
    val predictColumn = column(columnName).asInstanceOf[DataTableColumn[Double]]
    require(!predictColumn.hasNoneValues())
    val values = predictColumn.uniqueValues()
    require(values.toSet == Set(0.0, 1.0))
    val predictedActiveIndices = predictColumn.matchingIndices(Vector(1.0))
    val active = filterByColumnValues(columnName, Vector(1.0))
    val inactive = filterByColumnValues(columnName, Vector(0.0))
    assert(active.length + inactive.length == length)
    (inactive, active)
  }

  def rowToMap(row: Int): Map[String, Option[Any]] = {
    columns.map { c =>
      val v = c.values(row)
      c.title -> v
    }.toMap
  }

  def rows(): Vector[Map[String, Option[Any]]] = {
    Vector.range(0, length).map { rowNo =>
      rowToMap(rowNo)
    }
  }

}
