package com.cdd.models.vault

import java.util.Objects

import com.google.gson.annotations.Expose
import com.google.gson.annotations.SerializedName
import javax.annotation.Nullable
import com.cdd.models.utils.Modifier

import scala.annotation.meta.field
import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.reflect.ClassTag
//remove if not needed
import scala.collection.JavaConversions._

class Person(@(Nullable@field) var name: String)

trait ListContainer {
  val pageSize: Int
  val offset: Int
  val count: Int
}

class ObjectListContainer(@(SerializedName@field)("page_size") val pageSize: Int,
                          val offset: Int, val count: Int, val objects: Array[Object]) extends ListContainer {
  import VaultDownload.gson

  def convertObjects[A](implicit ctag: reflect.ClassTag[A]): Vector[A] = {
    objects.map { r =>
      val str = gson.toJson(r)
      gson.fromJson(str, ctag.runtimeClass.asInstanceOf[Class[A]])
    }.toVector
  }
}

class ProtocolListContainer(@(SerializedName@field)("page_size") val pageSize: Int,
                            val offset: Int, val count: Int,
                            @(SerializedName@field)("objects") val protocols: Array[Protocol]
                           ) extends ListContainer

class ProtocolDataContainer(@(SerializedName@field)("page_size") val pageSize: Int,
                            val offset: Int, val count: Int,
                            @(SerializedName@field)("objects") val data: Array[ProtocolData]
                           ) extends ListContainer

@SerialVersionUID(9174301962633611856L)
case class DataSet(name: String, id: Int) extends Serializable

@SerialVersionUID(1000L)
class Project(val name: String, val id: Int) extends Serializable

@SerialVersionUID(1000L)
class ProtocolData(val id: Int, val molecule: Int, val batch: Int, val run: Int,
                   @(SerializedName@field)("readouts") val readoutsTree: Object)

@SerialVersionUID(6048551671537459897L)
case class ReadoutValue[T](clazz: Class[T], protocolId: Int, readoutDefinitionId: Int, value: T,
                           modifier: Modifier = Modifier.EQUAL) extends Serializable

@SerialVersionUID(1000L)
class ProtocolDataReadouts(val protocolData: ProtocolData, val readoutValues: Vector[ReadoutValue[_]])

// These guys created by http://www.jsonschema2pojo.org/

@SerialVersionUID(8440937354210868315L)
class Protocol extends Serializable {

  @SerializedName("modified_at")
  @Expose
  @BeanProperty
  var modifiedAt: String = _

  @SerializedName("created_at")
  @Expose
  @BeanProperty
  var createdAt: String = _

  @SerializedName("owner")
  @Expose
  @BeanProperty
  var owner: String = _

  @SerializedName("projects")
  @Expose
  @BeanProperty
  var projects: Array[Project] = null

  @SerializedName("runs")
  @Expose
  @BeanProperty
  var runs: Array[Run] = null

  @SerializedName("readout_definitions")
  @Expose
  @BeanProperty
  var readoutDefinitions: Array[ReadoutDefinition] = null

  @SerializedName("data_set")
  @Expose
  @BeanProperty
  var dataSet: DataSet = _

  @SerializedName("category")
  @Expose
  @BeanProperty
  var category: String = _

  @SerializedName("description")
  @Expose
  @BeanProperty
  var description: String = _

  @SerializedName("name")
  @Expose
  @BeanProperty
  var name: String = _

  @SerializedName("class")
  @Expose
  private var _class: String = _

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: Int = _

  def getClass_(): String = _class

  def setClass_(_class: String) {
    this._class = _class
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[Protocol]

  override def equals(other: Any): Boolean = other match {
    case that: Protocol =>
      (that canEqual this) &&
        modifiedAt == that.modifiedAt &&
        createdAt == that.createdAt &&
        owner == that.owner &&
        dataSet == that.dataSet &&
        category == that.category &&
        description == that.description &&
        name == that.name &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(modifiedAt, createdAt, owner, dataSet, category, description, name, id)
    state.map(Objects.hashCode(_)).foldLeft(0)((a, b) => 31 * a + b)
  }
}

@SerialVersionUID(-5224149711231273285L)
class ReadoutDefinition extends Serializable {

  @SerializedName("protocol_condition")
  @Expose
  @BeanProperty
  var protocolCondition: java.lang.Boolean = _

  @SerializedName("description")
  @Expose
  @BeanProperty
  var description: String = _

  @SerializedName("precision_number")
  @Expose
  @BeanProperty
  var precisionNumber: java.lang.Integer = _

  @SerializedName("precision_type")
  @Expose
  @BeanProperty
  var precisionType: String = _

  @SerializedName("data_type")
  @Expose
  @BeanProperty
  var dataType: String = _

  @SerializedName("unit_label")
  @Expose
  @BeanProperty
  var unitLabel: String = _

  @SerializedName("name")
  @Expose
  @BeanProperty
  var name: String = _

  @SerializedName("class")
  @Expose
  private var _class: String = _

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: Int = _

  var nReadouts: Int = _

  def getClass_(): String = _class

  def setClass_(_class: String) {
    this._class = _class
  }


  def readoutType(): Class[_] = {
    dataType match {
      case "Text" => classOf[String]
      case "Number" => classOf[Double]
      case "Integer" => classOf[Int]
      case "PickList" => classOf[String]
      case _ =>
        throw new IllegalArgumentException(s"Unknown readout type for datatype $dataType")
    }
  }
}

@SerialVersionUID(2293772958622245905L)
class Run extends Serializable {

  @SerializedName("person")
  @Expose
  @BeanProperty
  var person: String = _

  @SerializedName("run_date")
  @Expose
  @BeanProperty
  var runDate: String = _

  @SerializedName("class")
  @Expose
  private var _class: String = _

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: Int = _

  def getClass_(): String = _class

  def setClass_(_class: String) {
    this._class = _class
  }
}

@SerialVersionUID(1000L)
class Batch extends Serializable {

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: java.lang.Integer = _

  @SerializedName("class")
  @Expose
  private var _class: String = _

  @SerializedName("name")
  @Expose
  @BeanProperty
  var name: String = _

  @SerializedName("owner")
  @Expose
  @BeanProperty
  var owner: String = _

  @SerializedName("created_at")
  @Expose
  @BeanProperty
  var createdAt: String = _

  @SerializedName("modified_at")
  @Expose
  @BeanProperty
  var modifiedAt: String = _

  @SerializedName("projects")
  @Expose
  @BeanProperty
  var projects: Array[Any] = null

  @SerializedName("salt_name")
  @Expose
  @BeanProperty
  var saltName: String = _

  @SerializedName("formula_weight")
  @Expose
  @BeanProperty
  var formulaWeight: java.lang.Double = _

  @SerializedName("batch_fields")
  @Expose
  @BeanProperty
  var batchFields: BatchFields = _

  def getClass_(): String = _class

  def setClass_(_class: String) {
    this._class = _class
  }
}

@SerialVersionUID(1000L)
class BatchFields extends Serializable {

  @SerializedName("ChEMBL_CMPD_NUMBER")
  @Expose
  @BeanProperty
  var chEMBLCMPDNUMBER: String = _

  @SerializedName("aring parent")
  @Expose
  @BeanProperty
  var aringParent: java.lang.Double = _

  @SerializedName("clogp parent")
  @Expose
  @BeanProperty
  var clogpParent: String = _

  @SerializedName("hba parent")
  @Expose
  @BeanProperty
  var hbaParent: java.lang.Double = _

  @SerializedName("hbd parent")
  @Expose
  @BeanProperty
  var hbdParent: String = _

  @SerializedName("heavy parent")
  @Expose
  @BeanProperty
  var heavyParent: String = _

  @SerializedName("tpsa parent")
  @Expose
  @BeanProperty
  var tpsaParent: String = _
}

@SerialVersionUID(1000L)
class Molecule extends Serializable {

  @SerializedName("id")
  @Expose
  @BeanProperty
  var id: java.lang.Integer = _

  @SerializedName("class")
  @Expose
  private var _class: String = _

  @SerializedName("name")
  @Expose
  @BeanProperty
  var name: String = _

  @SerializedName("synonyms")
  @Expose
  @BeanProperty
  var synonyms: Array[String] = null

  @SerializedName("cdd_registry_number")
  @Expose
  @BeanProperty
  var cddRegistryNumber: java.lang.Integer = _

  @SerializedName("projects")
  @Expose
  @BeanProperty
  var projects: Array[Any] = null

  @SerializedName("owner")
  @Expose
  @BeanProperty
  var owner: String = _

  @SerializedName("created_at")
  @Expose
  @BeanProperty
  var createdAt: String = _

  @SerializedName("modified_at")
  @Expose
  @BeanProperty
  var modifiedAt: String = _

  @SerializedName("smiles")
  @Expose
  @BeanProperty
  var smiles: String = _

  @SerializedName("cxsmiles")
  @Expose
  @BeanProperty
  var cxsmiles: String = _

  @SerializedName("inchi")
  @Expose
  @BeanProperty
  var inchi: String = _

  @SerializedName("inchi_key")
  @Expose
  @BeanProperty
  var inchiKey: String = _

  @SerializedName("iupac_name")
  @Expose
  @BeanProperty
  var iupacName: String = _

  @SerializedName("molfile")
  @Expose
  @BeanProperty
  var molfile: String = _

  @SerializedName("molecular_weight")
  @Expose
  @BeanProperty
  var molecularWeight: java.lang.Double = _

  @SerializedName("log_p")
  @Expose
  @BeanProperty
  var logP: java.lang.Double = _

  @SerializedName("log_d")
  @Expose
  @BeanProperty
  var logD: java.lang.Double = _

  @SerializedName("log_s")
  @Expose
  @BeanProperty
  var logS: java.lang.Double = _

  @SerializedName("num_h_bond_donors")
  @Expose
  @BeanProperty
  var numHBondDonors: java.lang.Integer = _

  @SerializedName("num_h_bond_acceptors")
  @Expose
  @BeanProperty
  var numHBondAcceptors: java.lang.Integer = _

  @SerializedName("num_rule_of_5_violations")
  @Expose
  @BeanProperty
  var numRuleOf5Violations: java.lang.Integer = _

  @SerializedName("formula")
  @Expose
  @BeanProperty
  var formula: String = _

  @SerializedName("isotope_formula")
  @Expose
  @BeanProperty
  var isotopeFormula: String = _

  @SerializedName("dot_disconnected_formula")
  @Expose
  @BeanProperty
  var dotDisconnectedFormula: String = _

  @SerializedName("p_k_a")
  @Expose
  private var pKA: java.lang.Double = _

  @SerializedName("p_k_a_type")
  @Expose
  private var pKAType: String = _

  @SerializedName("exact_mass")
  @Expose
  @BeanProperty
  var exactMass: java.lang.Double = _

  @SerializedName("heavy_atom_count")
  @Expose
  @BeanProperty
  var heavyAtomCount: java.lang.Integer = _

  @SerializedName("composition")
  @Expose
  @BeanProperty
  var composition: String = _

  @SerializedName("isotope_composition")
  @Expose
  @BeanProperty
  var isotopeComposition: String = _

  @SerializedName("topological_polar_surface_area")
  @Expose
  @BeanProperty
  var topologicalPolarSurfaceArea: java.lang.Double = _

  @SerializedName("num_rotatable_bonds")
  @Expose
  @BeanProperty
  var numRotatableBonds: java.lang.Integer = _

  @SerializedName("cns_mpo_score")
  @Expose
  @BeanProperty
  var cnsMpoScore: java.lang.Double = _

  @SerializedName("fsp3")
  @Expose
  @BeanProperty
  var fsp3: java.lang.Double = _

  @SerializedName("batches")
  @Expose
  @BeanProperty
  var batches: Array[Batch] = null

  def getClass_(): String = _class

  def setClass_(_class: String) {
    this._class = _class
  }

  def getPKA(): java.lang.Double = pKA

  def setPKA(pKA: java.lang.Double) {
    this.pKA = pKA
  }

  def getPKAType(): String = pKAType

  def setPKAType(pKAType: String) {
    this.pKAType = pKAType
  }
}