/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtils._
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtilsTest._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.Date

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeUtilsTest extends Specification {

  "The GeoMesaAvroProperty parser for" >> {
    "geometry format" should {
      "fail if the field does not have the required type" in {
        val field = invalidGeoMesaAvroSchema.getField("f1")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidFieldTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeoMesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeoMesaAvroSchema.getField("f4")
        GeoMesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeoMesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must beSome(GeoMesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeoMesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeoMesaAvroSchema.getField("f4")
        GeoMesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field1 = validGeoMesaAvroSchema.getField("f1")
        GeoMesaAvroGeomType.parse(field1) must beSome(classOf[Point])

        val field3 = validGeoMesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field3) must beSome(classOf[Geometry])
      }
    }

    "default geometry" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeoMesaAvroSchema.getField("f1")
        GeoMesaAvroGeomDefault.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "return a boolean value if valid" >> {
        val field = validGeoMesaAvroSchema.getField("f3")
        GeoMesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "date format" should {
      "fail if the field does not have the required type" in {
        val field1 = invalidGeoMesaAvroSchema.getField("f2")
        GeoMesaAvroDateFormat.parse(field1) must throwAn[AvroSimpleFeatureTypeUtils.InvalidFieldTypeException]

        val field2 = invalidGeoMesaAvroSchema.getField("f5")
        GeoMesaAvroDateFormat.parse(field2) must throwAn[AvroSimpleFeatureTypeUtils.InvalidFieldTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeoMesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "return a string value if valid" in {
        val field = validGeoMesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must beSome(GeoMesaAvroDateFormat.EPOCH_MILLIS)
      }
    }

    "feature visibility" should {
      "fail if the field does not have the required type" in {
        val field = invalidGeoMesaAvroSchema.getField("f2")
        GeoMesaAvroVisibilityField.parse(field) must throwAn[AvroSimpleFeatureTypeUtils.InvalidFieldTypeException]
      }

      "return a boolean value if valid" in {
        val field = validGeoMesaAvroSchema.getField("f6")
        GeoMesaAvroVisibilityField.parse(field) must beSome(true)
      }
    }
  }

  "The GeoMesaAvroProperty serializer for" >> {
    "geometry format" should {
      "fail if the value cannot be serialized because the format is invalid" in {
        val geom = generatePoint(40, 50)
        GeoMesaAvroGeomFormat.getSerializer(invalidGeoMesaAvroSchema, "f3").apply(geom) must
          throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "serialize a geometry with valid format metadata" in {
        "into wkt" in {
          val geom = generatePoint(40, 50)
          val expectedWkt = "POINT (40 50)"
          GeoMesaAvroGeomFormat.getSerializer(validGeoMesaAvroSchema, "f3").apply(geom) mustEqual expectedWkt
        }

        "into wkb" in {
          val geom = generatePoint(40, 50)
          val expectedWkb = WKBUtils.write(geom)
          GeoMesaAvroGeomFormat.getSerializer(validGeoMesaAvroSchema, "f1").apply(geom) mustEqual expectedWkb
        }
      }
    }

    "date format" should {
      "fail if the value cannot be serialized because the format is invalid" in {
        val date = new Date(1638915744897L)
        GeoMesaAvroDateFormat.getSerializer(invalidGeoMesaAvroSchema, "f4").apply(date) must
          throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
      }

      "serialize a date with valid format metadata" >> {
        "into a long" in {
          val expectedLong = 1638915744897L
          val date = new Date(expectedLong)
          GeoMesaAvroDateFormat.getSerializer(validGeoMesaAvroSchema, "f4").apply(date) mustEqual expectedLong
        }

        "into a string" in {
          val expectedString = "2021-12-07T17:22:24.000-05:00"
          val date = ISODateTimeFormat.dateTimeParser().parseDateTime(expectedString).toDate
          GeoMesaAvroDateFormat.getSerializer(validGeoMesaAvroSchema, "f5").apply(date) mustEqual expectedString
        }
      }
    }
  }

  "The GeoMesaAvroProperty deserializer for" >> {
    "geometry format" should {
      "fail if the value cannot be deserialized" >> {
        "because the format is invalid" in {
          val record = new GenericData.Record(invalidGeoMesaAvroSchema)
          record.put("f3", "POINT (10 20)")
          GeoMesaAvroGeomFormat.getDeserializer(record.getSchema, "f3").apply(record.get("f3")) must
            throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
        }

        "because the geometry cannot be parsed" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          record.put("f3", "POINT (0 0 0 0 0 0)")
          GeoMesaAvroGeomFormat.getDeserializer(record.getSchema, "f3").apply(record.get("f3")) must throwAn[Exception]
        }
      }

      "return the geometry if it can be deserialized" >> {
        "for a point" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          val expectedGeom = generatePoint(10, 20)
          record.put("f1", ByteBuffer.wrap(WKBUtils.write(expectedGeom)))
          GeoMesaAvroGeomFormat.getDeserializer(record.getSchema, "f1").apply(record.get("f1")) mustEqual expectedGeom
        }

        "for a geometry" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          val expectedGeom = generatePoint(20, 30).asInstanceOf[Geometry]
          record.put("f3", "POINT (20 30)")
          GeoMesaAvroGeomFormat.getDeserializer(record.getSchema, "f3").apply(record.get("f3")) mustEqual expectedGeom
        }
      }
    }

    "date format" should {
      "fail if the value cannot be deserialized" >> {
        "because the format is invalid" in {
          val record = new GenericData.Record(invalidGeoMesaAvroSchema)
          record.put("f4", "1638912032")
          GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f4").apply(record.get("f4")) must
            throwAn[AvroSimpleFeatureTypeUtils.InvalidPropertyValueException]
        }

        "because the date cannot be parsed" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          record.put("f5", "12/07/2021")
          GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f5").apply(record.get("f5")) must throwAn[Exception]
        }

        "because the type is incorrect" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          record.put("f4", 1000)
          GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f4").apply(record.get("f4")) must throwAn[Exception]
        }
      }

      "return the date if it can be deserialized" >> {
        "for milliseconds timestamp" in {
          val record = new GenericData.Record(validGeoMesaAvroSchema)
          val expectedDate = new Date(1638915744897L)
          record.put("f4", 1638915744897L)
          GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f4").apply(record.get("f4")) mustEqual expectedDate
        }

        "for an ISO datetime string with generic format" >> {

          "without millis" in {
            val record = new GenericData.Record(validGeoMesaAvroSchema)
            val expectedDate = new Date(1638915744000L)
            record.put("f5", "2021-12-07T17:22:24-05:00")
            GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f5").apply(record.get("f5")) mustEqual expectedDate
          }

          "with millis" in {
            val record = new GenericData.Record(validGeoMesaAvroSchema)
            val expectedDate = new Date(1638915744897L)
            record.put("f5", "2021-12-07T17:22:24.897-05:00")
            GeoMesaAvroDateFormat.getDeserializer(record.getSchema, "f5").apply(record.get("f5")) mustEqual expectedDate
          }
        }
      }
    }
  }

  "schemaToSft" should {
    "fail to convert a schema into an SFT" >> {
      "when there are invalid geomesa avro properties" in {
        AvroSimpleFeatureTypeUtils.schemaToSft(invalidGeoMesaAvroSchema) must
          throwAn[AvroSimpleFeatureTypeUtils.InvalidFieldTypeException]
      }

      "when there are multiple default geometry fields" in {
        val schemaJson =
          s"""{
             |  "type":"record",
             |  "name":"schema1",
             |  "fields":[
             |    {
             |      "name":"geom1",
             |      "type":"string",
             |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
             |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.LINESTRING}",
             |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
             |    },
             |    {
             |      "name":"geom2",
             |      "type":"bytes",
             |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
             |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.MULTIPOINT}",
             |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
             |    }
             |  ]
             |}""".stripMargin
        val schema = new Schema.Parser().parse(schemaJson)

        AvroSimpleFeatureTypeUtils.schemaToSft(schema) must throwAn[IllegalArgumentException]
      }

      "when there are multiple visibility fields" in {
        val schemaJson =
          s"""{
             |  "type":"record",
             |  "name":"schema1",
             |  "fields":[
             |    {
             |      "name":"visibility1",
             |      "type":"string",
             |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
             |    },
             |    {
             |      "name":"visibility2",
             |      "type":"string",
             |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
             |    }
             |  ]
             |}""".stripMargin
        val schema = new Schema.Parser().parse(schemaJson)

        AvroSimpleFeatureTypeUtils.schemaToSft(schema) must throwAn[IllegalArgumentException]
      }
    }

    "convert a schema into an SFT when the geomesa avro properties are valid" in {
      val sft = AvroSimpleFeatureTypeUtils.schemaToSft(validGeoMesaAvroSchema)

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual validSftSpec
    }
  }

  "sftToSchema" should {
    val typeName: String = "sft"

    "fail to convert an SFT into a schema" >> {
      "when there is an unsupported type" in {
        val sftSpec = "id:UUID,text:String"
        val sft = SimpleFeatureTypes.createType(typeName, sftSpec)

        AvroSimpleFeatureTypeUtils.sftToSchema(sft) must throwAn[UnsupportedAttributeTypeException]
      }

      "when a geometry field cannot be interpreted" >> {
        "because the format is missing" in {
          val sftSpec = s"id:String,position:Point:${SimpleFeatureTypes.AttributeOptions.OptNullable}=true"
          val sft = SimpleFeatureTypes.createType(typeName, sftSpec)

          AvroSimpleFeatureTypeUtils.sftToSchema(sft) must throwA[MissingPropertyException]
        }

        "because the format is invalid" in {
          val sftSpec = s"id:String,position:Point:${GeoMesaAvroGeomFormat.KEY}=twkb"
          val sft = SimpleFeatureTypes.createType(typeName, sftSpec)

          AvroSimpleFeatureTypeUtils.sftToSchema(sft) must throwA[InvalidPropertyValueException]
        }
      }

      "when a date field cannot be interpreted" >> {
        "because the format is missing" in {
          val sftSpec = "id:String,date:Date"
          val sft = SimpleFeatureTypes.createType(typeName, sftSpec)

          AvroSimpleFeatureTypeUtils.sftToSchema(sft) must throwA[MissingPropertyException]
        }

        "because the format is invalid" in {
          val sftSpec = s"id:String,date:Date:${GeoMesaAvroDateFormat.KEY}=timestamp"
          val sft = SimpleFeatureTypes.createType(typeName, sftSpec)

          AvroSimpleFeatureTypeUtils.sftToSchema(sft) must throwA[InvalidPropertyValueException]
        }
      }
    }

    "convert an SFT into a schema when the fields and user data are valid" in {
      val sft = SimpleFeatureTypes.createType(validGeoMesaAvroSchemaName, validSftSpec)

      val schema = AvroSimpleFeatureTypeUtils.sftToSchema(sft)

      schema.toString.sorted mustEqual validGeoMesaAvroSchemaInverse.sorted
    }
  }
}

object AvroSimpleFeatureTypeUtilsTest {

  private val invalidGeoMesaAvroSchemaName = "schema1"
  private val invalidGeoMesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"$invalidGeoMesaAvroSchemaName",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POLYGON}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATE}",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}"
       |    },
       |    {
       |      "name":"f3",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"TWKB",
       |      "${GeoMesaAvroGeomType.KEY}":"MultiGeometryCollection",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"dd-mm-yyyy",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeoMesaAvroSchema = new Schema.Parser().parse(invalidGeoMesaAvroSchemaJson)

  private val validGeoMesaAvroSchemaName = "schema2"
  private val validGeoMesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"$validGeoMesaAvroSchemaName",
       |  "geomesa.table.sharing":"false",
       |  "geomesa.table.compression.enabled":"true",
       |  "geomesa.index.dtg":"f4",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "index":"full",
       |      "cardinality":"high"
       |    },
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.FALSE}"
       |    },
       |    {
       |      "name":"f2",
       |      "type":["null","double"]
       |    },
       |    {
       |      "name":"f3",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeoMesaAvroGeomType.KEY}":"geometry",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"long",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":["null", "string"],
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATETIME}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"string",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    },
       |    {
       |      "name":"f7",
       |      "type":"int",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeoMesaAvroSchema: Schema = new Schema.Parser().parse(validGeoMesaAvroSchemaJson)
  private val validSftSpec = "id:String:cardinality=high:index=full,f1:Point:geomesa.geom.type=point:" +
    "geomesa.geom.default=false:geomesa.geom.format=wkb,f2:Double:nullable=true,*f3:Geometry:" +
    "geomesa.geom.type=geometry:geomesa.geom.default=true:geomesa.geom.format=wkt,f4:Date:" +
    "geomesa.date.format='epoch-millis',f5:Date:nullable=true:geomesa.date.format='iso-datetime';" +
    "geomesa.index.dtg='f4',geomesa.table.compression.enabled='true',geomesa.visibility.field='f6'," +
    "geomesa.table.sharing='false'"
  private val validGeoMesaAvroSchemaInverse = "{\"type\":\"record\",\"name\":\"schema2\",\"fields\":[{\"name\":" +
    "\"id\",\"type\":\"string\",\"index\":\"full\",\"cardinality\":\"high\"},{\"name\":\"f1\",\"type\":\"bytes\"," +
    "\"geomesa.geom.default\":\"false\",\"geomesa.geom.type\":\"point\",\"geomesa.geom.format\":\"wkb\"},{\"name\":" +
    "\"f2\",\"type\":[\"double\",\"null\"],\"nullable\":\"true\"},{\"name\":\"f3\",\"type\":\"string\"," +
    "\"geomesa.geom.default\":\"true\",\"geomesa.geom.type\":\"geometry\",\"geomesa.geom.format\":\"wkt\"}," +
    "{\"name\":\"f4\",\"type\":\"long\",\"geomesa.date.format\":\"epoch-millis\"},{\"name\":\"f5\",\"type\":" +
    "[\"string\",\"null\"],\"geomesa.date.format\":\"iso-datetime\",\"nullable\":\"true\"}]," +
    "\"geomesa.table.compression.enabled\":\"true\",\"geomesa.index.dtg\":\"f4\",\"geomesa.visibility.field\":" +
    "\"f6\",\"geomesa.table.sharing\":\"false\"}"

  private val geomFactory = new GeometryFactory()

  private def generatePoint(x: Double, y: Double): Point = geomFactory.createPoint(new Coordinate(x, y))
}
