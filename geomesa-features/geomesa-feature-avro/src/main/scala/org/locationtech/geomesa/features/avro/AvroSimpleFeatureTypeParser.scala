/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.{JsonProperties, Schema}
import org.apache.avro.generic.GenericRecord
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon}
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.ByteBuffer
import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.reflect.{ClassTag, classTag}

object AvroSimpleFeatureTypeParser {
  val reservedPropertyKeys: Set[String] = Set(
    GeoMesaAvroGeomFormat.KEY,
    GeoMesaAvroGeomType.KEY,
    GeoMesaAvroGeomDefault.KEY,
    GeoMesaAvroDateFormat.KEY,
    GeoMesaAvroVisibilityField.KEY,
    GeoMesaAvroExcludeField.KEY
  )

  /**
   * Convert an Avro [[Schema]] into a [[SimpleFeatureType]].
   */
  def schemaToSft(schema: Schema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(schema.getName)

    // any additional top-level props on the schema go in the sft user data
    val userData = schema.getProps

    schema.getFields.foreach { field =>
      val fieldName = field.name()
      val metadata = parseMetadata(field)

      var defaultGeomField: Option[String] = None

      if (!metadata.exclude) {
        metadata.field match {
          case GeometryField(_, geomType, default) =>
            builder.add(fieldName, geomType)
            if (default) {
              defaultGeomField.foreach { name =>
                throw new IllegalArgumentException(s"There may be only one default geometry: field '$name' was already " +
                  s"declared as the default")
              }
              builder.setDefaultGeometry(fieldName)
              defaultGeomField = Some(fieldName)
            }

          case DateField(_) =>
            builder.add(fieldName, classOf[Date])

          case StandardField =>
            addFieldToBuilder(builder, field)
        }

        userData.put(GeoMesaAvroVisibilityField.KEY, fieldName)
      }

      GeoMesaAvroVisibilityField.parse


      // any extra props on the field go in the attribute user data
      builder.get(fieldName).getUserData.putAll(metadata.extraProps)
    }

    val sft = builder.buildFeatureType()
    sft.getUserData.putAll(userData)
    sft
  }

  @tailrec
  private def addFieldToBuilder(builder: SimpleFeatureTypeBuilder,
                                field: Schema.Field,
                                typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema().getType) match {
      case Schema.Type.STRING  => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.BOOLEAN => builder.add(field.name(), classOf[java.lang.Boolean])
      case Schema.Type.INT     => builder.add(field.name(), classOf[java.lang.Integer])
      case Schema.Type.DOUBLE  => builder.add(field.name(), classOf[java.lang.Double])
      case Schema.Type.LONG    => builder.add(field.name(), classOf[java.lang.Long])
      case Schema.Type.FLOAT   => builder.add(field.name(), classOf[java.lang.Float])
      //how to support bytes type for non geometry fields?
      case Schema.Type.BYTES   => builder.add(field.name(), classOf[Array[Byte]])
      case Schema.Type.UNION   =>
        // if a union has more than one non-null type, it is not supported
        val types = field.schema().getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
        if (types.size != 1) {
          throw UnsupportedAvroTypeException(types.mkString("[", ", ", "]"))
        } else {
          addFieldToBuilder(builder, field, Option(types.head))
        }
      case Schema.Type.MAP     => throw UnsupportedAvroTypeException(Schema.Type.MAP.getName)
      case Schema.Type.RECORD  => throw UnsupportedAvroTypeException(Schema.Type.RECORD.getName)
      case Schema.Type.ENUM    => builder.add(field.name(), classOf[java.lang.String])
      case Schema.Type.ARRAY   => throw UnsupportedAvroTypeException(Schema.Type.ARRAY.getName)
      case Schema.Type.FIXED   => throw UnsupportedAvroTypeException(Schema.Type.FIXED.getName)
      case Schema.Type.NULL    => throw UnsupportedAvroTypeException(Schema.Type.NULL.getName)
      case _                   => throw UnsupportedAvroTypeException("unknown")
    }
  }

  private def parseMetadata(field: Schema.Field): GeoMesaAvroMetadata = {

    val extraProps = field.getProps.filterNot {
      case (key, _) => reservedPropertyKeys.contains(key)
    }.toMap

    val exclude = GeoMesaAvroExcludeField.parse(field).getOrElse(false)

    val geomFormat = GeoMesaAvroGeomFormat.parse(field)
    val geomType = GeoMesaAvroGeomType.parse(field)
    val geomDefault = GeoMesaAvroGeomDefault.parse(field).getOrElse(false)

    if (geomFormat.isDefined && geomType.isDefined) {
      return GeoMesaAvroMetadata(GeometryField(geomFormat.get, geomType.get, geomDefault), extraProps, exclude)
    }

    val dateFormat = GeoMesaAvroDateFormat.parse(field)

    if (dateFormat.isDefined) {
      return GeoMesaAvroMetadata(DateField(dateFormat.get), extraProps, exclude)
    }

    GeoMesaAvroMetadata(StandardField, extraProps, exclude)
  }

  private case class GeoMesaAvroMetadata(field: GeoMesaAvroField, extraProps: Map[String, String], exclude: Boolean)

  private sealed trait GeoMesaAvroField
  private case class GeometryField(format: String, typ: Class[_ <: Geometry], default: Boolean) extends GeoMesaAvroField
  private case class DateField(format: String) extends GeoMesaAvroField
  private case object StandardField extends GeoMesaAvroField

  case class UnsupportedAvroTypeException(typeName: String)
    extends IllegalArgumentException(s"Type '$typeName' is not supported for SFT conversion")

  /**
   * An attribute in an Avro [[Schema]] to provide additional information when creating an SFT.
   *
   * @tparam K the type of properties to which this property applies
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeomesaAvroProperty[K <: JsonProperties, T] {
    /**
     * The key in the properties for this attribute.
     */
    val KEY: String

    /**
     * Parse the value from the properties at this property's `key`.
     *
     * @return `None` if the `key` does not exist, else the value at the `key`
     */
    def parse(properties: K): Option[T]

    protected final def assertFieldType(schema: Schema, typ: Schema.Type): Unit = {
      schema.getType match {
        case Schema.Type.UNION =>
          // if a union has more than one non-null type, it should not be converted to an SFT
          val unionTypes = schema.getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
          if (unionTypes.size != 1 || typ != unionTypes.head) {
            throw GeomesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
        case fieldType: Schema.Type =>
          if (typ != fieldType) {
            throw GeomesaAvroProperty.InvalidPropertyTypeException(typ.getName, KEY)
          }
      }
    }
  }

  object GeomesaAvroProperty {
    final case class InvalidPropertyValueException(value: String, key: String)
      extends IllegalArgumentException(s"Unable to parse value '$value' for property '$key'")

    final case class InvalidPropertyTypeException(typeName: String, key: String)
      extends IllegalArgumentException(s"Schemas with property '$key' must have type '$typeName'")
  }

  /**
   * A [[GeomesaAvroProperty]] that has a finite set of possible string values.
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeomesaAvroEnumProperty[T] extends GeomesaAvroProperty[T] {
    // case clauses to match the values of the enum and possibly check the field type
    protected val matcher: PartialFunction[(String, Schema.Field), T]

    override final def parse(schema: Schema): Option[T] = {
      Option(schema.getProp(KEY)).map(_.toLowerCase(Locale.ENGLISH)).map { value =>
        matcher.lift.apply((value, field)).getOrElse {
          throw GeomesaAvroProperty.InvalidPropertyValueException(value, KEY)
        }
      }
    }
  }

  /**
   * A [[GeomesaAvroEnumProperty]] that parses to a boolean value.
   */
  trait GeomesaAvroBooleanProperty extends GeomesaAvroEnumProperty[Boolean] {
    final val TRUE: String = "true"
    final val FALSE: String = "false"

    override protected val matcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, _) => true
      case (FALSE, _) => false
    }
  }

  /**
   * A [[GeomesaAvroEnumProperty]] that can be deserialized from a avro record.
   *
   * @tparam T the type of the value to be parsed from this property
   * @tparam K the type of the value to be deserialized from this property
   */
  abstract class GeomesaAvroDeserializableEnumProperty[T, K >: Null: ClassTag] extends GeomesaAvroEnumProperty[T] {
    // case clauses to match the values of the enum and deserialize the data accordingly
    protected val dematcher: PartialFunction[(T, AnyRef), K]

    final def deserialize(record: GenericRecord, fieldName: String): K = {
      val data = record.get(fieldName)
      if (data == null) { return null }
      try {
        parse(record.getSchema.getField(fieldName)).map { value =>
          dematcher.lift.apply((value, data)).getOrElse {
            throw GeomesaAvroProperty.InvalidPropertyValueException(value.toString, KEY)
          }
        }.getOrElse {
          throw GeomesaAvroDeserializableEnumProperty.MissingPropertyValueException[K](fieldName, KEY)
        }
      } catch {
        case ex: Exception => throw GeomesaAvroDeserializableEnumProperty.DeserializationException[K](fieldName, ex)
      }
    }
  }

  object GeomesaAvroDeserializableEnumProperty {
    final case class MissingPropertyValueException[K: ClassTag](fieldName: String, key: String)
      extends RuntimeException(s"Cannot process field '$fieldName' for type '${classTag[K].runtimeClass.getName}' " +
        s"because key '$key' is missing")

    final case class DeserializationException[K: ClassTag](fieldName: String, t: Throwable)
      extends RuntimeException(s"Cannot deserialize field '$fieldName' into a '${classTag[K].runtimeClass.getName}': " +
        s"${t.getMessage}")
  }

  /**
   * Indicates that this field should be interpreted as a [[Geometry]], with one of the formats specified below.
   */
  object GeoMesaAvroGeomFormat extends GeomesaAvroDeserializableEnumProperty[String, Geometry] {
    override val KEY: String = "geomesa.geom.format"

    /**
     * Well-Known Text representation as a [[String]]
     */
    val WKT: String = "wkt"
    /**
     * Well-Known Bytes representation as an [[Array]] of [[Byte]]s
     */
    val WKB: String = "wkb"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (WKT, field) => assertFieldType(field, Schema.Type.STRING); WKT
      case (WKB, field) => assertFieldType(field, Schema.Type.BYTES); WKB
    }

    override protected val dematcher: PartialFunction[(String, AnyRef), Geometry] = {
      case (WKT, data) => WKTUtils.read(data.toString)
      case (WKB, data) => WKBUtils.read(data.asInstanceOf[ByteBuffer].array())
    }
  }

  /**
   * Indicates that this field represents a [[Geometry]], with one of the types specified below.
   */
  object GeoMesaAvroGeomType extends GeomesaAvroEnumProperty[Class[_ <: Geometry]] {
    override val KEY: String = "geomesa.geom.type"

    /**
     * A [[Geometry]]
     */
    val GEOMETRY: String = "geometry"
    /**
     * A [[Point]]
     */
    val POINT: String = "point"
    /**
     * A [[LineString]]
     */
    val LINESTRING: String = "linestring"
    /**
     * A [[Polygon]]
     */
    val POLYGON: String = "polygon"
    /**
     * A [[MultiPoint]]
     */
    val MULTIPOINT: String = "multipoint"
    /**
     * A [[MultiLineString]]
     */
    val MULTILINESTRING: String = "multilinestring"
    /**
     * A [[MultiPolygon]]
     */
    val MULTIPOLYGON: String = "multipolygon"
    /**
     * A [[GeometryCollection]]
     */
    val GEOMETRYCOLLECTION: String = "geometrycollection"

    override protected val matcher: PartialFunction[(String, Schema.Field), Class[_ <: Geometry]] = {
      case (GEOMETRY, _) => classOf[Geometry]
      case (POINT, _) => classOf[Point]
      case (LINESTRING, _) => classOf[LineString]
      case (POLYGON, _) => classOf[Polygon]
      case (MULTIPOINT, _) => classOf[MultiPoint]
      case (MULTILINESTRING, _) => classOf[MultiLineString]
      case (MULTIPOLYGON, _) => classOf[MultiPolygon]
      case (GEOMETRYCOLLECTION, _) => classOf[GeometryCollection]
    }
  }

  /**
   * Indicates that this field should be interpreted as the default [[Geometry]] for this [[SimpleFeatureType]].
   */
  object GeoMesaAvroGeomDefault extends GeomesaAvroBooleanProperty {
    override val KEY: String = "geomesa.geom.default"
  }

  /**
   * Indicates that the field should be interpreted as a [[Date]], with one of the formats specified below.
   */
  object GeoMesaAvroDateFormat extends GeomesaAvroDeserializableEnumProperty[String, Date] {
    override val KEY: String = "geomesa.date.format"

    /**
     * Milliseconds since the Unix epoch as a [[Long]]
     */
    val EPOCH_MILLIS: String = "epoch-millis"
    /**
     * A [[String]] with generic ISO date format
     */
    val ISO_DATE: String = "iso-date"
    /**
     * A [[String]] with date format "yyyy-MM-dd"
     */
    val ISO_FULL_DATE: String = "iso-full-date"
    /**
     * A [[String]] with generic ISO datetime format
     */
    val ISO_DATETIME: String = "iso-datetime"
    /**
     * A [[String]] with date format "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
     */
    val ISO_INSTANT: String = "iso-instant"
    /**
     * A [[String]] with date format "yyyy-MM-dd'T'HH:mm:ssZZ"
     */
    val ISO_INSTANT_NO_MILLIS: String = "iso-instant-no-millis"

    override protected val matcher: PartialFunction[(String, Schema.Field), String] = {
      case (EPOCH_MILLIS, field) => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
      case (ISO_DATE, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATE
      case (ISO_FULL_DATE, field) => assertFieldType(field, Schema.Type.STRING); ISO_FULL_DATE
      case (ISO_DATETIME, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATETIME
      case (ISO_INSTANT, field) => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT
      case (ISO_INSTANT_NO_MILLIS, field) => assertFieldType(field, Schema.Type.STRING); ISO_INSTANT_NO_MILLIS
    }

    override protected val dematcher: PartialFunction[(String, AnyRef), Date] = {
      case (EPOCH_MILLIS, data) => new Date(data.asInstanceOf[java.lang.Long])
      case (ISO_DATE, data) => ISODateTimeFormat.dateParser().parseDateTime(data.toString).toDate
      case (ISO_FULL_DATE, data) => ISODateTimeFormat.date().parseDateTime(data.toString).toDate
      case (ISO_DATETIME, data) => ISODateTimeFormat.dateTimeParser().parseDateTime(data.toString).toDate
      case (ISO_INSTANT, data) => ISODateTimeFormat.dateTime().parseDateTime(data.toString).toDate
      case (ISO_INSTANT_NO_MILLIS, data) => ISODateTimeFormat.dateTimeNoMillis().parseDateTime(data.toString).toDate
    }
  }

  /**
   * Specifies the name of the avro field to be as the visibility for features of this [[SimpleFeatureType]].
   */
  object GeoMesaAvroVisibilityField extends GeomesaAvroProperty[String] {
    override val KEY: String = "geomesa.visibility.field"

    override def parse(field: Schema.Field): Option[String] = {
      Option(field.getProp(KEY)).map {
        name => assertFieldType(field, Schema.Type.STRING); name
      }
    }
  }

  /**
   * Specifies whether this field should be included in the [[SimpleFeatureType]].
   */
  object GeoMesaAvroExcludeField extends GeomesaAvroBooleanProperty {
    override val KEY: String = "geomesa.exclude.field"
  }
}
