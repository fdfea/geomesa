/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.generic.GenericRecord
import org.apache.avro.{Schema, SchemaBuilder}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import java.nio.ByteBuffer
import java.util.{Date, Locale}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Try

object AvroSimpleFeatureTypeUtils {

  // reserved avro keywords should not be set as metadata on an avro schema, nor included as SFT user data
  val reservedAvroProperties = Set("name", "namespace", "doc", "aliases", "fields", "name", "type", "default", "order",
    "symbols", "items", "values", "size", "logicalType", "precision", "scale")

  /**
   * Convert an Avro [[Schema]] into a [[SimpleFeatureType]].
   */
  def schemaToSft(schema: Schema): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(schema.getName)

    // any extra props on the schema go in the SFT user data
    val sftUserData = schema.getProps.filterNot {
      case (key, _) => reservedAvroProperties.contains(key)
    }

    var defaultGeomField: Option[String] = None
    var visibilityField: Option[String] = None

    schema.getFields.foreach { field =>
      val fieldName = field.name
      val metadata = parseMetadata(field)
      val nullable = isFieldNullable(field)

      metadata.field match {
        case GeometryField(_, geomType, default) if !metadata.exclude =>
          builder.add(fieldName, geomType)
          if (default) {
            defaultGeomField.foreach { name =>
              throw new IllegalArgumentException(s"There may be only one default geometry field in a schema: " +
                s"'$name' was already declared as the default")
            }
            builder.setDefaultGeometry(fieldName)
            defaultGeomField = Some(fieldName)
          }

        case DateField(_) if !metadata.exclude =>
          builder.add(fieldName, classOf[Date])

        case VisibilityField =>
          if (!metadata.exclude) {
            addFieldToSft(builder, field)
          }
          visibilityField.foreach { name =>
            throw new IllegalArgumentException(s"There may be only one visibility field in a schema: " +
              s"'$name' was already declared as the visibility")
          }
          sftUserData.put(GeoMesaAvroVisibilityField.KEY, fieldName)
          visibilityField = Some(fieldName)

        case StandardField if !metadata.exclude =>
          addFieldToSft(builder, field)

        case _ =>
      }

      if (!metadata.exclude) {
        // any props on the schema field go in the attribute user data
        builder.get(fieldName).getUserData.putAll(metadata.extraProps)

        if (nullable) {
          builder.get(fieldName).getUserData.put(SimpleFeatureTypes.AttributeOptions.OptNullable, nullable.toString)
        }
      }
    }

    val sft = builder.buildFeatureType()
    sft.getUserData.putAll(sftUserData)
    sft
  }

  /**
   * Convert a [[SimpleFeatureType]] into an Avro [[Schema]].
   */
  def sftToSchema(sft: SimpleFeatureType): Schema = {

    val recordBuilder = SchemaBuilder.record(sft.getTypeName)

    // set user data on the sft as props on the schema
    sft.getUserData.foreach {
      case (key: String, value: String) => recordBuilder.prop(key, value)
      case _ =>
    }

    val fieldAssembler = recordBuilder.fields()

    sft.getAttributeDescriptors.foreach(addFieldToSchema(fieldAssembler, _))

    fieldAssembler.endRecord()
  }

  @tailrec
  private def addFieldToSft(builder: SimpleFeatureTypeBuilder,
                            field: Schema.Field,
                            typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema.getType) match {
      case Schema.Type.STRING  => builder.add(field.name, classOf[java.lang.String])
      case Schema.Type.BOOLEAN => builder.add(field.name, classOf[java.lang.Boolean])
      case Schema.Type.INT     => builder.add(field.name, classOf[java.lang.Integer])
      case Schema.Type.DOUBLE  => builder.add(field.name, classOf[java.lang.Double])
      case Schema.Type.LONG    => builder.add(field.name, classOf[java.lang.Long])
      case Schema.Type.FLOAT   => builder.add(field.name, classOf[java.lang.Float])
      case Schema.Type.BYTES   => builder.add(field.name, classOf[Array[Byte]])
      case Schema.Type.UNION   =>
        // if a union has more than one non-null type, it is not supported
        val types = field.schema.getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
        if (types.size != 1) {
          throw UnsupportedSchemaTypeException(field.name, types.mkString("[", ", ", "]"))
        } else {
          addFieldToSft(builder, field, Option(types.head))
        }
      case Schema.Type.MAP     => throw UnsupportedSchemaTypeException(field.name, Schema.Type.MAP.getName)
      case Schema.Type.RECORD  => throw UnsupportedSchemaTypeException(field.name, Schema.Type.RECORD.getName)
      case Schema.Type.ENUM    => builder.add(field.name, classOf[java.lang.String])
      case Schema.Type.ARRAY   => throw UnsupportedSchemaTypeException(field.name, Schema.Type.ARRAY.getName)
      case Schema.Type.FIXED   => throw UnsupportedSchemaTypeException(field.name, Schema.Type.FIXED.getName)
      case Schema.Type.NULL    => throw UnsupportedSchemaTypeException(field.name, Schema.Type.NULL.getName)
      case _                   => throw UnsupportedSchemaTypeException(field.name, "unknown")
    }
  }

  private def addFieldToSchema(fieldAssembler: SchemaBuilder.FieldAssembler[Schema],
                               descriptor: AttributeDescriptor): Unit = {
    val fieldName = descriptor.getLocalName
    val fieldBuilder = fieldAssembler.name(fieldName)

    val userData = descriptor.getUserData.flatMap {
      case (key: String, value: String) => Some(key -> value)
      case _ => None
    }.filterNot {
      case (key, _) => reservedAvroProperties.contains(key)
    }

    // set user data on the attribute as props on the schema field
    userData.foreach { case (key: String, value: String) =>
      fieldBuilder.prop(key, value)
    }

    val nullable = userData.get(SimpleFeatureTypes.AttributeOptions.OptNullable)
      .flatMap(s => Try(s.toBoolean).toOption).getOrElse(false)

    val typeBuilder =
      if (nullable) {
        fieldBuilder.`type`().nullable()
      } else {
        fieldBuilder.`type`()
      }

    descriptor.getType.getBinding match {
      case binding if classOf[java.lang.String].isAssignableFrom(binding) =>
        typeBuilder.stringType().noDefault()
      case binding if classOf[java.lang.Boolean].isAssignableFrom(binding) =>
        typeBuilder.booleanType().noDefault()
      case binding if classOf[java.lang.Integer].isAssignableFrom(binding) =>
        typeBuilder.intType().noDefault()
      case binding if classOf[java.lang.Double].isAssignableFrom(binding) =>
        typeBuilder.doubleType().noDefault()
      case binding if classOf[java.lang.Long].isAssignableFrom(binding) =>
        typeBuilder.longType().noDefault()
      case binding if classOf[java.lang.Float].isAssignableFrom(binding) =>
        typeBuilder.floatType().noDefault()
      case binding if classOf[Array[Byte]].isAssignableFrom(binding) =>
        typeBuilder.bytesType().noDefault()
      case binding if classOf[Geometry].isAssignableFrom(binding) =>
        userData.get(GeoMesaAvroGeomFormat.KEY).map {
          case GeoMesaAvroGeomFormat.WKT => typeBuilder.stringType().noDefault()
          case GeoMesaAvroGeomFormat.WKB => typeBuilder.bytesType().noDefault()
          case value: String => throw InvalidPropertyValueException(fieldName, GeoMesaAvroGeomFormat.KEY, value)
        }.getOrElse {
          throw MissingPropertyException(fieldName, GeoMesaAvroGeomFormat.KEY)
        }
      case binding if classOf[Date].isAssignableFrom(binding) =>
        userData.get(GeoMesaAvroDateFormat.KEY).map {
          case GeoMesaAvroDateFormat.EPOCH_MILLIS => typeBuilder.longType().noDefault()
          case GeoMesaAvroDateFormat.ISO_DATE => typeBuilder.stringType().noDefault()
          case GeoMesaAvroDateFormat.ISO_DATETIME => typeBuilder.stringType().noDefault()
          case value: String => throw InvalidPropertyValueException(fieldName, GeoMesaAvroDateFormat.KEY, value)
        }.getOrElse {
          throw MissingPropertyException(fieldName, GeoMesaAvroDateFormat.KEY)
        }
      case binding: Class[_] => throw UnsupportedAttributeTypeException(fieldName, binding.getTypeName)
    }
  }

  private def parseMetadata(field: Schema.Field): GeoMesaAvroMetadata = {
    lazy val geomFormat = GeoMesaAvroGeomFormat.parse(field)
    lazy val geomType = GeoMesaAvroGeomType.parse(field)
    lazy val geomDefault = GeoMesaAvroGeomDefault.parse(field).getOrElse(false)
    lazy val dateFormat = GeoMesaAvroDateFormat.parse(field)
    lazy val visibility = GeoMesaAvroVisibilityField.parse(field).getOrElse(false)

    val metadata =
      if (geomFormat.isDefined && geomType.isDefined) {
        GeometryField(geomFormat.get, geomType.get, geomDefault)
      } else if (dateFormat.isDefined) {
        DateField(dateFormat.get)
      } else if (visibility) {
        VisibilityField
      } else {
        StandardField
      }

    val extraProps = field.getProps.filterNot {
      case (key, _) => reservedAvroProperties.contains(key)
    }.toMap

    val exclude = GeoMesaAvroExcludeField.parse(field).getOrElse(false)

    GeoMesaAvroMetadata(metadata, extraProps, exclude)
  }

  private def isFieldNullable(field: Schema.Field): Boolean = {
    field.schema.getType == Schema.Type.UNION && field.schema.getTypes.map(_.getType).contains(Schema.Type.NULL)
  }

  private case class GeoMesaAvroMetadata(field: GeoMesaAvroField, extraProps: Map[String, String], exclude: Boolean)

  private sealed trait GeoMesaAvroField
  private case class GeometryField(format: String, typ: Class[_ <: Geometry], default: Boolean) extends GeoMesaAvroField
  private case class DateField(format: String) extends GeoMesaAvroField
  private case object VisibilityField extends GeoMesaAvroField
  private case object StandardField extends GeoMesaAvroField

  case class UnsupportedSchemaTypeException(field: String, `type`: String)
    extends IllegalArgumentException(s"Avro Schema type '${`type`}' on field '$field' is not supported")

  case class UnsupportedAttributeTypeException(field: String, `type`: String)
    extends IllegalArgumentException(s"SimpleFeature attribute type '${`type`}' on field '$field' is not supported")

  case class InvalidPropertyValueException(field: String, key: String, value: String)
    extends IllegalArgumentException(s"Unable to parse value '$value' for property '$key' on field '$field'")

  case class InvalidFieldTypeException(field: String, key: String, `type`: String)
    extends IllegalArgumentException(s"Field '$field' with property '$key' must have type '${`type`}'")

  case class MissingPropertyException(field: String, key: String)
    extends IllegalArgumentException(s"Key '$key' is missing for field '$field'")

  /**
   * A property in an Avro [[Schema.Field]] to provide additional information when generating a [[SimpleFeatureType]].
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeoMesaAvroProperty[T] {
    /**
     * The key in the [[Schema.Field]] for this property.
     */
    val KEY: String

    /**
     * Parse the value from the [[Schema.Field]] properties at this property's `KEY`.
     *
     * @return `None` if the `KEY` does not exist, else the value at the `KEY`
     */
    def parse(field: Schema.Field): Option[T]

    protected final def assertFieldType(field: Schema.Field, typ: Schema.Type): Unit = {
      field.schema.getType match {
        case Schema.Type.UNION =>
          // if a union has more than one non-null type, it should not be converted to an SFT
          val unionTypes = field.schema.getTypes.map(_.getType).filter(_ != Schema.Type.NULL).toSet
          if (unionTypes.size != 1 || typ != unionTypes.head) {
            throw InvalidFieldTypeException(field.name, KEY, typ.getName)
          }
        case fieldType: Schema.Type =>
          if (typ != fieldType) {
            throw InvalidFieldTypeException(field.name, KEY, typ.getName)
          }
      }
    }
  }

  /**
   * A [[GeoMesaAvroProperty]] that has a finite set of possible string values.
   *
   * @tparam T the type of the value to be parsed from this property
   */
  trait GeoMesaAvroEnumProperty[T] extends GeoMesaAvroProperty[T] {
    // case clauses to match the values of the enum and possibly check the field type
    protected val valueMatcher: PartialFunction[(String, Schema.Field), T]

    override final def parse(field: Schema.Field): Option[T] = {
      Option(field.getProp(KEY)).map(_.toLowerCase(Locale.ENGLISH)).map { value =>
        valueMatcher.lift.apply((value, field)).getOrElse {
          throw InvalidPropertyValueException(field.name, KEY, value)
        }
      }
    }
  }

  /**
   * A [[GeoMesaAvroEnumProperty]] that parses to a boolean value.
   */
  trait GeoMesaAvroBooleanProperty extends GeoMesaAvroEnumProperty[Boolean] {
    final val TRUE: String = "true"
    final val FALSE: String = "false"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, _) => true
      case (FALSE, _) => false
    }
  }

  /**
   * A [[GeoMesaAvroEnumProperty]] with a value that can be deserialized from an Avro [[GenericRecord]].
   *
   * @tparam T the type of the value to be parsed from this property
   * @tparam K the type of the value to be deserialized from this property
   */
  abstract class GeoMesaAvroSerDesEnumProperty[T, K] extends GeoMesaAvroEnumProperty[T] {
    // case clauses to match the value of the enum to a function to serialize the data
    protected val serializerMatcher: PartialFunction[T, K => AnyRef]

    // case clauses to match the value of the enum to a function to deserialize the data
    protected val deserializerMatcher: PartialFunction[T, AnyRef => K]

    final def getSerializer(schema: Schema, fieldName: String): K => AnyRef = {
      applyFieldMatcher(schema, fieldName, serializerMatcher)
    }

    final def getDeserializer(schema: Schema, fieldName: String): AnyRef => K = {
      applyFieldMatcher(schema, fieldName, deserializerMatcher)
    }

    private def applyFieldMatcher[R](schema: Schema, fieldName: String, func: PartialFunction[T, R]): R = {
      parse(schema.getField(fieldName)).map { value =>
        func.lift.apply(value).getOrElse {
          throw InvalidPropertyValueException(fieldName, KEY, value.toString)
        }
      }.getOrElse {
        throw MissingPropertyException(fieldName, KEY)
      }
    }
  }

  /**
   * Indicates that this field should be interpreted as a [[Geometry]] with the given format. This property must be
   * accompanied by the [[GeoMesaAvroGeomType]] property.
   */
  object GeoMesaAvroGeomFormat extends GeoMesaAvroSerDesEnumProperty[String, Geometry] {
    override val KEY: String = "geomesa.geom.format"

    /**
     * Well-Known Text representation as a [[String]]
     */
    val WKT: String = "wkt"
    /**
     * Well-Known Bytes representation as an [[Array]] of [[Byte]]s
     */
    val WKB: String = "wkb"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), String] = {
      case (WKT, field) => assertFieldType(field, Schema.Type.STRING); WKT
      case (WKB, field) => assertFieldType(field, Schema.Type.BYTES); WKB
    }

    override protected val serializerMatcher: PartialFunction[String, Geometry => AnyRef] = {
      case WKT => geom => WKTUtils.write(geom)
      case WKB => geom => WKBUtils.write(geom)
    }

    override protected val deserializerMatcher: PartialFunction[String, AnyRef => Geometry] = {
      case WKT => data => WKTUtils.read(data.toString)
      case WKB => data => WKBUtils.read(data.asInstanceOf[ByteBuffer].array())
    }
  }

  /**
   * Indicates that this field represents a [[Geometry]] with the given type. This property must be accompanied
   * by the [[GeoMesaAvroGeomFormat]] property.
   */
  object GeoMesaAvroGeomType extends GeoMesaAvroEnumProperty[Class[_ <: Geometry]] {
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

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Class[_ <: Geometry]] = {
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
   * Indicates that this field is the default [[Geometry]] for this [[SimpleFeatureType]]. This property must be
   * accompanied by the [[GeoMesaAvroGeomFormat]] and [[GeoMesaAvroGeomType]] properties, and there may only be one
   * of these properties for a given schema.
   */
  object GeoMesaAvroGeomDefault extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.geom.default"
  }

  /**
   * Indicates that the field should be interpreted as a [[Date]] in the given format.
   */
  object GeoMesaAvroDateFormat extends GeoMesaAvroSerDesEnumProperty[String, Date] {
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
     * A [[String]] with generic ISO datetime format
     */
    val ISO_DATETIME: String = "iso-datetime"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), String] = {
      case (EPOCH_MILLIS, field) => assertFieldType(field, Schema.Type.LONG); EPOCH_MILLIS
      case (ISO_DATE, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATE
      case (ISO_DATETIME, field) => assertFieldType(field, Schema.Type.STRING); ISO_DATETIME
    }

    override protected val serializerMatcher: PartialFunction[String, Date => AnyRef] = {
      case EPOCH_MILLIS => date => date.getTime.asInstanceOf[java.lang.Long]
      case ISO_DATE => date => ISODateTimeFormat.date.print(date.getTime)
      case ISO_DATETIME => date => ISODateTimeFormat.dateTime.print(date.getTime)
    }

    override protected val deserializerMatcher: PartialFunction[String, AnyRef => Date] = {
      case EPOCH_MILLIS => data => new Date(data.asInstanceOf[java.lang.Long])
      case ISO_DATE => data => ISODateTimeFormat.dateParser.parseDateTime(data.toString).toDate
      case ISO_DATETIME => data => ISODateTimeFormat.dateTimeParser.parseDateTime(data.toString).toDate
    }
  }

  /**
   * Specifies that the value of this field should be used as the visibility for features of this [[SimpleFeatureType]].
   * There may only be one of these properties for a given schema.
   */
  object GeoMesaAvroVisibilityField extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.visibility.field"

    override protected val valueMatcher: PartialFunction[(String, Schema.Field), Boolean] = {
      case (TRUE, field) => assertFieldType(field, Schema.Type.STRING); true
      case (FALSE, field) => assertFieldType(field, Schema.Type.STRING); false
    }
  }

  /**
   * Specifies whether this field should be excluded from the [[SimpleFeatureType]].
   */
  object GeoMesaAvroExcludeField extends GeoMesaAvroBooleanProperty {
    override val KEY: String = "geomesa.exclude.field"
  }
}
