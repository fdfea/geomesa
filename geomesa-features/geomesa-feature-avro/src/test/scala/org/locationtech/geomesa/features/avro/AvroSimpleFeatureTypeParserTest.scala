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
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroCardinality, GeomesaAvroDateDefault, GeoMesaAvroDateFormat, GeomesaAvroDeserializableEnumProperty, GeoMesaAvroFeatureVisibility, GeoMesaAvroGeomDefault, GeoMesaAvroGeomFormat, GeomesaAvroGeomSrid, GeoMesaAvroGeomType, GeomesaAvroIndex, GeomesaAvroProperty}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory
import org.locationtech.jts.geom.{Coordinate, CoordinateSequence, Geometry, GeometryFactory, Point}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.Date

@RunWith(classOf[JUnitRunner])
class AvroSimpleFeatureTypeParserTest extends Specification {

  private val invalidGeomesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes",
       |      "${GeomesaAvroGeomSrid.KEY}":"3401",
       |      "${GeomesaAvroIndex.KEY}":"no",
       |      "${GeomesaAvroCardinality.KEY}":"medium"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POLYGON}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"yes",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATE}",
       |      "${GeoMesaAvroFeatureVisibility.KEY}":"${GeoMesaAvroFeatureVisibility.TRUE}"
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
       |      "${GeoMesaAvroDateFormat.KEY}":"dd-mm-yyyy"
       |    },
       |    {
       |      "name":"f5",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    }
       |  ]
       |}""".stripMargin
  private val invalidGeomesaAvroSchema = new Schema.Parser().parse(invalidGeomesaAvroSchemaJson)

  private val validGeomesaAvroSchemaJson =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "geomesa.table.sharing":"false",
       |  "geomesa.table.compression.enabled":"true",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "${GeomesaAvroIndex.KEY}":"${GeomesaAvroIndex.FULL}",
       |      "${GeomesaAvroCardinality.KEY}":"${GeomesaAvroCardinality.HIGH}"
       |    },
       |    {
       |      "name":"f1",
       |      "type":"bytes",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.FALSE}",
       |      "${GeomesaAvroGeomSrid.KEY}":"${GeomesaAvroGeomSrid.EPSG_4326}",
       |      "${GeomesaAvroDateDefault.KEY}":"${GeomesaAvroDateDefault.FALSE}"
       |    },
       |    {
       |      "name":"f2",
       |      "type":"double"
       |    },
       |    {
       |      "name":"f3",
       |      "type":["null","string"],
       |      "${GeoMesaAvroGeomFormat.KEY}":"wkt",
       |      "${GeoMesaAvroGeomType.KEY}":"geometry",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"f4",
       |      "type":"long",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}",
       |      "${GeomesaAvroDateDefault.KEY}":"${GeomesaAvroDateDefault.TRUE}"
       |    },
       |    {
       |      "name":"f5",
       |      "type":["null","string"],
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_INSTANT}"
       |    },
       |    {
       |      "name":"f6",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATETIME}"
       |    },
       |    {
       |      "name":"f7",
       |      "type":"string",
       |      "${GeoMesaAvroFeatureVisibility.KEY}":"${GeoMesaAvroFeatureVisibility.TRUE}"
       |    }
       |  ]
       |}""".stripMargin
  private val validGeomesaAvroSchema: Schema = new Schema.Parser().parse(validGeomesaAvroSchemaJson)

  private val geomFactory = new GeometryFactory()
  private val coordinateFactory = CoordinateArraySequenceFactory.instance()

  private def generateCoordinate(x: Double, y: Double): CoordinateSequence = {
    coordinateFactory.create(Array(new Coordinate(x, y)))
  }

  "The GeomesaAvroProperty parser for" >> {
    "geometry format" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroGeomFormat.parse(field) must beNone
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomFormat.parse(field) must beSome(GeoMesaAvroGeomFormat.WKT)
      }
    }

    "geometry type" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return None if the property doesn't exist" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroGeomType.parse(field) must beNone
      }

      "return a geometry type if valid" in {
        val field1 = validGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomType.parse(field1) must beSome(classOf[Point])

        val field3 = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomType.parse(field3) must beSome(classOf[Geometry])
      }
    }

    "default geometry" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeoMesaAvroGeomDefault.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a boolean value if valid" >> {
        val field = validGeomesaAvroSchema.getField("f3")
        GeoMesaAvroGeomDefault.parse(field) must beSome(true)
      }
    }

    "geometry srid" should {
      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomSrid.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a string if valid" in {
        val field = validGeomesaAvroSchema.getField("f1")
        GeomesaAvroGeomSrid.parse(field) must beSome("4326")
      }
    }

    "date format" should {
      "fail if the field does not have the required type(s)" in {
        val field1 = invalidGeomesaAvroSchema.getField("f2")
        GeoMesaAvroDateFormat.parse(field1) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]

        val field2 = invalidGeomesaAvroSchema.getField("f5")
        GeoMesaAvroDateFormat.parse(field2) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "fail if an unsupported value is parsed" in {
        val field = invalidGeomesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyValueException]
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f4")
        GeoMesaAvroDateFormat.parse(field) must beSome(GeoMesaAvroDateFormat.EPOCH_MILLIS)
      }
    }

    "index" should {
      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("id")
        GeomesaAvroIndex.parse(field) must beSome(GeomesaAvroIndex.FULL)
      }
    }

    "cardinality" should {
      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("id")
        GeomesaAvroCardinality.parse(field) must beSome(GeomesaAvroCardinality.HIGH)
      }
    }

    "feature visibility" should {
      "fail if the field does not have the required type(s)" in {
        val field = invalidGeomesaAvroSchema.getField("f2")
        GeoMesaAvroFeatureVisibility.parse(field) must throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
      }

      "return a string value if valid" in {
        val field = validGeomesaAvroSchema.getField("f7")
        GeoMesaAvroFeatureVisibility.parse(field) must beSome(true)
      }
    }
  }

  "The GeomesaAvroProperty deserializer for " >> {
    "geometry format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f3", "POINT(10 20)")
        GeoMesaAvroGeomFormat.deserialize(record, "f3") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Geometry]]
      }

      "fail if the value cannot be deserialized because the geometry cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f3", "POINT(0 0 0 0 0 0)")
        GeoMesaAvroGeomFormat.deserialize(record, "f3") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Geometry]]
      }

      "return the geometry if it can be deserialized" >> {
        "for a point" in {
          val record1 = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom1 = new Point(generateCoordinate(10, 20), geomFactory)
          record1.put("f1", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
          GeoMesaAvroGeomFormat.deserialize(record1, "f1") mustEqual expectedGeom1
        }

        "for a geometry" in {
          val record2 = new GenericData.Record(validGeomesaAvroSchema)
          val expectedGeom2 = new Point(generateCoordinate(10, 20), geomFactory).asInstanceOf[Geometry]
          record2.put("f3", "POINT(10 20)")
          GeoMesaAvroGeomFormat.deserialize(record2, "f3") mustEqual expectedGeom2
        }
      }
    }

    "date format" should {
      "fail if the value cannot be deserialized because the format is invalid" in {
        val record = new GenericData.Record(invalidGeomesaAvroSchema)
        record.put("f4", "1638912032")
        GeoMesaAvroDateFormat.deserialize(record, "f4") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the date cannot be parsed" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f5", "12/07/2021")
        GeoMesaAvroDateFormat.deserialize(record, "f5") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "fail if the value cannot be deserialized because the type is incorrect" in {
        val record = new GenericData.Record(validGeomesaAvroSchema)
        record.put("f4", 1000)
        GeoMesaAvroDateFormat.deserialize(record, "f4") must
          throwA[GeomesaAvroDeserializableEnumProperty.DeserializationException[Date]]
      }

      "return the date if it can be deserialized" >> {
        "for milliseconds timestamp" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedDate = new Date(1638915744897L)
          record.put("f4", 1638915744897L)
          GeoMesaAvroDateFormat.deserialize(record, "f4") mustEqual expectedDate
        }

        "for a null string" in {
          val record = new GenericData.Record(validGeomesaAvroSchema)
          val expectedDate = null
          record.put("f5", null)
          GeoMesaAvroDateFormat.deserialize(record, "f5") mustEqual expectedDate
        }

        "for an ISO datetime string" >> {

          "with milliseconds" in {
            val record = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate = new Date(1638915744897L)
            record.put("f5", "2021-12-07T17:22:24.897-05:00")
            GeoMesaAvroDateFormat.deserialize(record, "f5") mustEqual expectedDate
          }

          "without generic format" in {
            val record1 = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate1 = new Date(1638915744000L)
            record1.put("f6", "2021-12-07T17:22:24-05:00")
            GeoMesaAvroDateFormat.deserialize(record1, "f6") mustEqual expectedDate1

            val record2 = new GenericData.Record(validGeomesaAvroSchema)
            val expectedDate2 = new Date(1638915744897L)
            record2.put("f6", "2021-12-07T17:22:24.897-05:00")
            GeoMesaAvroDateFormat.deserialize(record2, "f6") mustEqual expectedDate2
          }
        }
      }
    }
  }

  "AvroSimpleFeatureParser" should {
    "fail to convert a schema with invalid geomesa avro properties into an SFT" in {
      AvroSimpleFeatureTypeParser.schemaToSft(invalidGeomesaAvroSchema) must
        throwAn[GeomesaAvroProperty.InvalidPropertyTypeException]
    }

    "convert a schema with valid geomesa avro properties into an SFT" in {
      val expectedSft = "id:String:cardinality=high:index=full,f1:Point:srid=4326,f2:Double,*f3:Geometry,f4:Date," +
        "f5:Date,f6:Date,f7:String;geomesa.index.dtg='f4',geomesa.table.compression.enabled='true'," +
        "geomesa.avro.visibility.field='f7',geomesa.table.sharing='false'"
      val sft = AvroSimpleFeatureTypeParser.schemaToSft(validGeomesaAvroSchema)

      SimpleFeatureTypes.encodeType(sft, includeUserData = true) mustEqual expectedSft
    }
  }
}
