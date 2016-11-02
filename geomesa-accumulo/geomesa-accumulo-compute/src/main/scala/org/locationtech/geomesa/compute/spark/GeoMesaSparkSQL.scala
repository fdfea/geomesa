package org.locationtech.geomesa.compute.spark

import java.sql.Timestamp
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SQLTypes}
import org.geotools.data._
import org.geotools.factory.CommonFactoryFinder
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

// Spark DataSource for GeoMesa
// enables loading a GeoMesa DataFrame as
// {{
// val df = spark.read
//   .format("geomesa")
//   .option(GM.instanceIdParam.getName, "mycloud")
//   .option(GM.userParam.getName, "user")
//   .option(GM.passwordParam.getName, "password")
//   .option(GM.tableNameParam.getName, "sparksql")
//   .option(GM.mockParam.getName, "true")
//   .option("geomesa.feature", "chicago")
//   .load()
// }}
class GeoMesaDataSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider {

  override def shortName(): String = "geomesa"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    SQLTypes.init(sqlContext)
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters("geomesa.feature"))
    val schema = sft2StructType(sft)
    new GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val ds = DataStoreFinder.getDataStore(parameters)
    val sft = ds.getSchema(parameters("geomesa.feature"))
    new GeoMesaRelation(sqlContext, sft, schema, parameters)
  }

  private def sft2StructType(sft: SimpleFeatureType) = {
    val fields = sft.getAttributeDescriptors.map { ad => ad2field(ad) }.toList
    StructType(StructField("__fid__", DataTypes.StringType, nullable =false) :: fields)
  }

  private def ad2field(ad: AttributeDescriptor): StructField = {
    import java.{lang => jl}
    val dt = ad.getType.getBinding match {
      case t if t == classOf[jl.Double]                       => DataTypes.DoubleType
      case t if t == classOf[jl.Float]                        => DataTypes.FloatType
      case t if t == classOf[jl.Integer]                      => DataTypes.IntegerType
      case t if t == classOf[jl.String]                       => DataTypes.StringType
      case t if t == classOf[jl.Boolean]                      => DataTypes.BooleanType
      case t if      classOf[Geometry].isAssignableFrom(t)    => SQLTypes.PointType
      case t if t == classOf[java.util.Date]                  => DataTypes.TimestampType
      case _                                                  => null
    }
    StructField(ad.getLocalName, dt)
  }
}


// the Spark Relation that builds the scan over the GeoMesa table
// used by the SQL optimization rules to push spatio-temporal predicates into the `filt` variable
case class GeoMesaRelation(sqlContext: SQLContext,
                           sft: SimpleFeatureType,
                           schema: StructType,
                           params: Map[String, String],
                           filt: org.opengis.filter.Filter = org.opengis.filter.Filter.INCLUDE,
                           props: Option[Seq[String]] = None)
  extends BaseRelation with PrunedFilteredScan {

  lazy val isMock = params("useMock").toBoolean

  override def buildScan(requiredColumns: Array[String], filters: Array[org.apache.spark.sql.sources.Filter]): RDD[Row] = {
    SparkUtils.buildScan(sft, requiredColumns, filters, org.opengis.filter.Filter.INCLUDE, sqlContext.sparkContext, schema, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter {
      case t @ (_:IsNotNull | _:IsNull) => true
      case _ => false
    }
  }
}

object SparkUtils {

  @transient val ff = CommonFactoryFinder.getFilterFactory2

  def buildScan(sft: SimpleFeatureType,
                requiredColumns: Array[String],
                filters: Array[org.apache.spark.sql.sources.Filter],
                filt: org.opengis.filter.Filter,
                ctx: SparkContext,
                schema: StructType,
                params: Map[String, String]): RDD[Row] = {
    val compiledCQL = filters.flatMap(sparkFilterToCQLFilter).foldLeft[org.opengis.filter.Filter](org.opengis.filter.Filter.INCLUDE) { (l, r) => ff.and(l, r) }
    val rdd = GeoMesaSpark.rdd(
      new Configuration(), ctx, params,
      new Query(params("geomesa.feature"), compiledCQL),
      params("useMock").toBoolean, Option.empty[Int])

    val requiredIndexes = requiredColumns.map {
      case "__fid__" => -1
      case col => sft.indexOf(col)
    }
    val result = rdd.map(SparkUtils.sf2row(schema, _, requiredIndexes))
    result.asInstanceOf[RDD[Row]]
  }

  def sparkFilterToCQLFilter(filt: org.apache.spark.sql.sources.Filter): Option[org.opengis.filter.Filter] = filt match {
    case GreaterThanOrEqual(attribute, v) => Some(ff.greaterOrEqual(ff.property(attribute), ff.literal(v)))
    case GreaterThan(attr, v)             => Some(ff.greater(ff.property(attr), ff.literal(v)))
    case LessThanOrEqual(attr, v)         => Some(ff.lessOrEqual(ff.property(attr), ff.literal(v)))
    case LessThan(attr, v)                => Some(ff.less(ff.property(attr), ff.literal(v)))
    case EqualTo(attr, v)                 => Some(ff.equals(ff.property(attr), ff.literal(v)))
    case In(attr, values)                 => Some(values.map(v => ff.equals(ff.property(attr), ff.literal(v))).reduce[org.opengis.filter.Filter]( (l,r) => ff.or(l,r)))
    case And(left, right)                 => Some(ff.and(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get)) // TODO: can these be null
    case Or(left, right)                  => Some(ff.or(sparkFilterToCQLFilter(left).get, sparkFilterToCQLFilter(right).get))
    case Not(f)                           => Some(ff.not(sparkFilterToCQLFilter(f).get))
    case StringStartsWith(a, v)           => Some(ff.like(ff.property(a), s"$v%"))
    case StringEndsWith(a, v)             => Some(ff.like(ff.property(a), s"%$v"))
    case StringContains(a, v)             => Some(ff.like(ff.property(a), s"%$v%"))
    case IsNull(attr)                     => None
    case IsNotNull(attr)                  => None
  }

  def sf2row(schema: StructType, sf: SimpleFeature, requiredIndexes: Array[Int]): Row = {
    val res = Array.ofDim[Any](requiredIndexes.length)
    var i = 0
    while(i < requiredIndexes.length) {
      val idx = requiredIndexes(i)
      if(idx == -1) res(i) = sf.getID
      else res(i) = toSparkType(sf.getAttribute(idx))
      i += 1
    }
    new GenericRowWithSchema(res, schema)
  }

  // TODO: optimize so we're not type checking every value
  def toSparkType(v: Any): AnyRef = v match {
    case t: Date => new Timestamp(t.getTime)
    case t       => t.asInstanceOf[AnyRef]
  }

}
