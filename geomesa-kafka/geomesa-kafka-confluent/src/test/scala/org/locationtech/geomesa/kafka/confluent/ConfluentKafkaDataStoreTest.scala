/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.geotools.data.{DataStoreFinder, Transaction}
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtils._
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreTest._
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.{Date, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends Specification {
  sequential

  "ConfluentKafkaDataStore" should {
    "write simple features to avro" in new ConfluentKafkaTestContext {
      private val sft = SimpleFeatureTypes.createType(topic, encodedSft2)
      sft.getUserData.put(SimpleFeatureTypes.Configs.MixedGeometries, "true")
      KafkaDataStore.setTopic(sft, topic) //is this needed?

      private val kds = getStore()
      kds.createSchema(sft)

      private val id = "1"
      private val geom = generatePoint(2, 2)
      private val date = new Date(1638915744897L)
      private val expectedGeom = ByteBuffer.wrap(WKBUtils.write(geom))
      private val expectedDate = date.getTime

      private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
      private val threads = Executors.newFixedThreadPool(1)
      threads.execute(pollTopic(topic, messageBuffer))

      private val feature = ScalaSimpleFeature.create(sft, id, geom, date)

      WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)

        eventually(20, 100.millis) {
          val records = messageBuffer.toList
          records.size mustEqual 1

          val record = records.head
          record.key mustEqual id
          record.value.get("shape") mustEqual expectedGeom
          record.value.get("date") mustEqual expectedDate
        }
      }

      threads.shutdownNow()
    }

    /*
    "write simple features to avro then modify them" in new ConfluentKafkaTestContext {
      private val sft = SimpleFeatureTypes.createType(topic, encodedSft2)
      sft.getUserData.put(SimpleFeatureTypes.Configs.MixedGeometries, "true")
      KafkaDataStore.setTopic(sft, topic) //is this needed?

      private val kds = getStore()
      kds.createSchema(sft)

      private val id = "1"
      private val geom = generatePoint(2, 2)
      private val date = new Date(1638915744897L)
      private val expectedGeom = ByteBuffer.wrap(WKBUtils.write(geom))
      private val expectedDate = date.getTime

      private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
      private val threads = Executors.newFixedThreadPool(1)
      threads.execute(pollTopic(topic, messageBuffer))

      private val feature = ScalaSimpleFeature.create(sft, id, geom, date)

      WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)
      }

      private val fs = kds.getFeatureSource(topic)
      private val features = fs.getFeatures.features

      eventually(20, 100.millis) {
        SelfClosingIterator(features).toArray.length mustEqual 1

        val feature = features.next
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
        feature.getID mustEqual id
        feature.getAttribute("shape") mustEqual expectedGeom
        feature.getAttribute("date") mustEqual expectedDate
      }

      private val geom_updated = generatePoint(2, 2)
      private val date_updated = new Date(1638915744897L)
      private val expectedGeom_updated = ByteBuffer.wrap(WKBUtils.write(geom_updated))
      private val expectedDate_updated = date_updated.getTime

      private val feature_updated = ScalaSimpleFeature.create(sft, id, geom_updated, date_updated)

      WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature_updated, useProvidedFid = true)
      }

      private val features_updated = fs.getFeatures.features

      eventually(20, 100.millis) {
        SelfClosingIterator(features_updated).toArray.length mustEqual 1

        val feature = features_updated.next
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
        feature.getID mustEqual id
        feature.getAttribute("shape") mustEqual expectedGeom_updated
        feature.getAttribute("date") mustEqual expectedDate_updated
      }
    }

    "read simple features from avro" in new ConfluentKafkaTestContext {
      private val id1 = "1"
      private val expectedPosition1 = generatePoint(10d, 20d)
      private val expectedSpeed1 = 12.325d
      private val expectedDate1 = new Date(1638915744897L)
      private val expectedVisibility1 = ""

      private val record1 = new GenericData.Record(schema1)
      record1.put("id", id1)
      record1.put("position", "POINT (10 20)")
      record1.put("speed", expectedSpeed1)
      record1.put("date", "2021-12-07T17:22:24.897-05:00")
      record1.put("visibility", expectedVisibility1)

      private val id2 = "2"

      private val record2 = new GenericData.Record(schema1)
      record2.put("id", id1)
      record2.put("position", "POINT (15 35)")
      record2.put("speed", 0.00d)
      record2.put("date", "2021-12-07T17:23:25.372-05:00")
      record2.put("visibility", "hidden")

      private val producer = getProducer[String, GenericRecord]()
      producer.send(new ProducerRecord(topic, id1, record1)).get
      producer.send(new ProducerRecord(topic, id2, record2)).get

      private val kds = getStore()
      private val fs = kds.getFeatureSource(topic)
      private val features = fs.getFeatures.features

      eventually(20, 100.millis) {
        // the record with "hidden" visibility doesn't appear because no auths are configured
        SelfClosingIterator(features).toArray.length mustEqual 1

        val feature = features.next
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft1
        feature.getID mustEqual id1
        feature.getAttribute("position") mustEqual expectedPosition1
        feature.getAttribute("speed") mustEqual expectedSpeed1
        feature.getAttribute("date") mustEqual expectedDate1
        feature.getDefaultGeometry mustEqual expectedPosition1
        SecurityUtils.getVisibility(feature) mustEqual expectedVisibility1
      }
    }

    "read simple features then write avro" >> {
      "when there are no excluded fields" in new ConfluentKafkaTestContext {
        private val id_read = "1"
        private val expectedGeom_read = generatePoint(10d, 20d)
        private val expectedDate_read = new Date(1639145281285L)

        private val record_read = new GenericData.Record(schema2)
        record_read.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom_read)))
        record_read.put("date", expectedDate_read.getTime)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id_read, record_read)).get

        private val kds = getStore()
        private val fs_read = kds.getFeatureSource(topic)
        private val features_read = fs_read.getFeatures.features

        // read feature and check fields
        eventually(20, 100.millis) {
          SelfClosingIterator(features_read).toArray.length mustEqual 1

          val feature = features_read.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id_read
          feature.getAttribute("shape") mustEqual expectedGeom_read
          feature.getAttribute("date") mustEqual expectedDate_read
        }

        private val sft = kds.getSchema(topic)

        private val id_write = "2"
        private val geom_write = generatePoint(2, 2)
        private val date_write = new Date(1638915744897L)
        private val expectedGeom_write = ByteBuffer.wrap(WKBUtils.write(geom_write))
        private val expectedDate_write = date_write.getTime

        private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
        private val threads = Executors.newFixedThreadPool(1)
        threads.execute(pollTopic(topic, messageBuffer))

        private val feature_write = ScalaSimpleFeature.create(sft, id_write, geom_write, date_write)

        // write feature and check fields
        WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature_write, useProvidedFid = true)

          eventually(20, 100.millis) {
            val records = messageBuffer.toList
            records.size mustEqual 1

            val record = records.head
            record.key mustEqual id_write
            record.value.get("shape") mustEqual expectedGeom_write
            record.value.get("date") mustEqual expectedDate_write
          }
        }

        threads.shutdownNow()
      }

      "when there are excluded fields" in new ConfluentKafkaTestContext {
        private val id_read = "1"
        private val expectedSpeed_read = 12.325d
        private val expectedGeom_read = generatePoint(10d, 20d)
        private val expectedDate_read = new Date(1638915744897L)
        //private val expectedVisibility_read = ""

        private val record_read = new GenericData.Record(schema1)
        record_read.put("id", id_read)
        record_read.put("position", WKTUtils.write(expectedGeom_read))
        record_read.put("speed", expectedSpeed_read)
        record_read.put("date", ISODateTimeFormat.dateTime.print(expectedDate_read))
        //record_read.put("visibility", expectedVisibility_read)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id_read, record_read)).get

        private val kds = getStore()
        private val fs_read = kds.getFeatureSource(topic)
        private val features_read = fs_read.getFeatures.features

        // read feature and check fields
        eventually(20, 100.millis) {
          SelfClosingIterator(features_read).toArray.length mustEqual 1
          val feature = features_read.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft1
          feature.getID mustEqual id_read
          feature.getAttribute("position") mustEqual expectedGeom_read
          feature.getAttribute("speed") mustEqual expectedSpeed_read
          feature.getAttribute("date") mustEqual expectedDate_read
          feature.getDefaultGeometry mustEqual expectedGeom_read
          //SecurityUtils.getVisibility(feature) mustEqual expectedVisibility_read
        }

        private val sft = kds.getSchema(topic)

        private val id_write = "2"
        private val geom_write = generatePoint(2, 2)
        private val date_write = new Date(1638915744897L)
        private val expectedSpeed_write = 71.33d
        private val expectedGeom_write = WKTUtils.write(geom_write)
        private val expectedDate_write = date_write.getTime
        //private val expectedVisibility_write = ""

        private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
        private val threads = Executors.newFixedThreadPool(1)
        threads.execute(pollTopic(topic, messageBuffer))

        // visibility field doesn't exist in SFT
        private val feature_write = ScalaSimpleFeature.create(sft, id_write, geom_write, expectedSpeed_write, date_write)

        // write feature and check fields
        WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature_write, useProvidedFid = true)

          eventually(20, 100.millis) {
            val records = messageBuffer.toList
            records.size mustEqual 1

            val record = records.head
            record.key mustEqual id_write
            record.value.get("position") mustEqual expectedGeom_write
            record.value.get("speed") mustEqual expectedSpeed_write
            record.value.get("date") mustEqual expectedDate_write
            //record.value.get("visibility") mustEqual expectedVisibility_write
          }
        }

        threads.shutdownNow()
      }
    }

    "write avro then read simple features" >> {
      "when there are no excluded fields" in new ConfluentKafkaTestContext {
        private val sft = SimpleFeatureTypes.createType(topic, encodedSft2)
        sft.getUserData.put(SimpleFeatureTypes.Configs.MixedGeometries, "true")
        KafkaDataStore.setTopic(sft, topic) //is this needed?

        private val kds = getStore()
        kds.createSchema(sft)

        private val id_write = "1"
        private val geom_write = generatePoint(2, 2)
        private val date_write = new Date(1638915744897L)
        private val expectedGeom_write = ByteBuffer.wrap(WKBUtils.write(geom_write))
        private val expectedDate_write = date_write.getTime

        private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
        private val threads = Executors.newFixedThreadPool(1)
        threads.execute(pollTopic(topic, messageBuffer))

        private val feature = ScalaSimpleFeature.create(sft, id_write, geom_write, date_write)

        // write feature and check fields
        WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature, useProvidedFid = true)

          eventually(20, 100.millis) {
            val records = messageBuffer.toList
            records.size mustEqual 1
            val record = records.head
            record.key mustEqual id_write
            record.value.get("shape") mustEqual expectedGeom_write
            record.value.get("date") mustEqual expectedDate_write
          }
        }

        threads.shutdownNow()

        private val id_read = "2"
        private val date_read = 1639145281285L
        private val expectedGeom_read = generatePoint(10d, 20d)
        private val expectedDate_read = new Date(date_read)

        private val record = new GenericData.Record(schema2)
        record.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom_read)))
        record.put("date", date_read)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id_read, record)).get

        private val fs_read = kds.getFeatureSource(topic)
        private val features_read = fs_read.getFeatures.features

        // read feature and check fields
        eventually(20, 100.millis) {
          SelfClosingIterator(features_read).toArray.length mustEqual 1
          val feature = features_read.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id_read
          feature.getAttribute("shape") mustEqual expectedGeom_read
          feature.getAttribute("date") mustEqual expectedDate_read
        }
      }

      "when there are excluded fields" in new ConfluentKafkaTestContext {
        private val sft = SimpleFeatureTypes.createType(topic, encodedSft2)
        sft.getUserData.put(SimpleFeatureTypes.Configs.MixedGeometries, "true")
        KafkaDataStore.setTopic(sft, topic) //is this needed?

        private val kds = getStore()
        kds.createSchema(sft)

        private val id_write = "1"
        private val geom_write = generatePoint(2, 2)
        private val date_write = new Date(1638915744897L)
        private val expectedSpeed_write = 71.33d
        private val expectedGeom_write = WKTUtils.write(geom_write)
        private val expectedDate_write = date_write.getTime
        //private val expectedVisibility_write = ""

        private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
        private val threads = Executors.newFixedThreadPool(1)
        threads.execute(pollTopic(topic, messageBuffer))

        // visibility field doesn't exist in SFT
        private val feature_write = ScalaSimpleFeature.create(sft, id_write, geom_write, expectedSpeed_write, date_write)

        // write feature and check fields
        WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.write(writer, feature_write, useProvidedFid = true)

          eventually(20, 100.millis) {
            val records = messageBuffer.toList
            records.size mustEqual 1

            val record = records.head
            record.key mustEqual id_write
            record.value.get("position") mustEqual expectedGeom_write
            record.value.get("speed") mustEqual expectedSpeed_write
            record.value.get("date") mustEqual expectedDate_write
            //record.value.get("visibility") mustEqual expectedVisibility_write
          }
        }

        threads.shutdownNow()

        private val id_read = "1"
        private val expectedSpeed_read = 12.325d
        private val expectedGeom_read = generatePoint(10d, 20d)
        private val expectedDate_read = new Date(1638915744897L)
        //private val expectedVisibility_read = ""

        private val record_read = new GenericData.Record(schema1)
        record_read.put("id", id_read)
        record_read.put("position", WKTUtils.write(expectedGeom_read))
        record_read.put("speed", expectedSpeed_read)
        record_read.put("date", ISODateTimeFormat.dateTime.print(expectedDate_read))
        //record_read.put("visibility", expectedVisibility_read)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id_read, record_read)).get

        private val fs_read = kds.getFeatureSource(topic)
        private val features_read = fs_read.getFeatures.features

        // read feature and check fields
        eventually(20, 100.millis) {
          SelfClosingIterator(features_read).toArray.length mustEqual 1
          val feature = features_read.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft1
          feature.getID mustEqual id_read
          feature.getAttribute("position") mustEqual expectedGeom_read
          feature.getAttribute("speed") mustEqual expectedSpeed_read
          feature.getAttribute("date") mustEqual expectedDate_read
          feature.getDefaultGeometry mustEqual expectedGeom_read
          //SecurityUtils.getVisibility(feature) mustEqual expectedVisibility_read
        }
      }
    }

    "support the GeoMessage control operation for" >> {
      "update" in new ConfluentKafkaTestContext {
        private val id = "1"
        private val expectedGeom1 = generatePoint(10d, 20d)
        private val expectedDate1 = new Date(1639145281285L)

        private val record1 = new GenericData.Record(schema2)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", expectedDate1.getTime)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id, record1)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)
        private val features1 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features1).toArray.length mustEqual 1

          val feature = features1.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom1
          feature.getAttribute("date") mustEqual expectedDate1
        }

        private val expectedGeom2 = generatePoint(15d, 30d)
        private val expectedDate2 = null

        private val record2 = new GenericData.Record(schema2)
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", expectedDate2)

        producer.send(new ProducerRecord[String, GenericRecord](topic, id, record2)).get

        private val features2 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features2).toArray.length mustEqual 1

          val feature = features2.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual expectedDate2
        }
      }

      "drop" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val expectedGeom1 = generatePolygon(Seq((0d, 0d), (10d, 0d), (10d, 10d), (0d, 10d), (0d, 0d)))
        private val expectedDate1 = new Date(1639145281285L)

        private val record1 = new GenericData.Record(schema2)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", expectedDate1.getTime)

        private val id2 = "2"
        private val expectedGeom2 = generatePolygon(Seq((5d, 5d), (15d, 5d), (15d, 15d), (5d, 15d), (5d, 5d)))
        private val expectedDate2 = new Date(1639145515643L)

        private val record2 = new GenericData.Record(schema2)
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", expectedDate2.getTime)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id1, record1)).get
        producer.send(new ProducerRecord(topic, id2, record2)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)
        private val features1 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features1).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord(topic, id1, null)).get

        private val features2 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features2).toArray.length mustEqual 1

          val feature = features2.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id2
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual expectedDate2
        }
      }

      "clear" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val expectedGeom1 = generatePolygon(Seq((0d, 0d), (10d, 0d), (10d, 10d), (0d, 10d), (0d, 0d)))
        private val expectedDate1 = new Date(1639145281285L)

        private val record1 = new GenericData.Record(schema2)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", expectedDate1.getTime)

        private val id2 = "2"
        private val expectedGeom2 = generatePolygon(Seq((5d, 5d), (15d, 5d), (15d, 15d), (5d, 15d), (5d, 5d)))
        private val expectedDate2 = new Date(1639145515643L)

        private val record2 = new GenericData.Record(schema2)
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", expectedDate2.getTime)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id1, record1)).get
        producer.send(new ProducerRecord(topic, id2, record2)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)
        private val features1 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features1).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord[String, GenericRecord](topic, "", null)).get

        private val features2 = fs.getFeatures.features

        eventually(20, 100.millis) {
          SelfClosingIterator(features2).toArray.length mustEqual 0
        }
      }
    }

    "fail to get a feature source when the schema cannot be converted to an SFT" in new ConfluentKafkaTestContext {
      private val record = new GenericData.Record(badSchema)
      record.put("f1", "POINT (10 20)")

      private val producer = getProducer[String, GenericRecord]()
      producer.send(new ProducerRecord(topic, null, record)).get

      private val kds = getStore()

      // the geometry field cannot be parsed so the SFT never gets created and CKDS can't find the schema
      kds.getFeatureSource(topic) must throwAn[Exception]
    }

    "fail to get a feature writer when the SFT cannot be converted to a schema" in new ConfluentKafkaTestContext {
      // uuid is not a supported type for schema conversion
      private val badSftSpec = "id:UUID,*geom:Point"
      private val sft = SimpleFeatureTypes.createType(topic, badSftSpec)
      KafkaDataStore.setTopic(sft, topic) //is this needed?

      private val kds = getStore()
      kds.createSchema(sft)

      kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT) must throwAn[Exception]
    }
     */
  }
}

private object ConfluentKafkaDataStoreTest {

  val topic: String = "confluent_kds_test"

  val schemaJson1: String =
    s"""{
       |  "type":"record",
       |  "name":"schema1",
       |  "geomesa.index.dtg":"date",
       |  "fields":[
       |    {
       |      "name":"id",
       |      "type":"string",
       |      "cardinality":"high"
       |    },
       |    {
       |      "name":"position",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKT}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.POINT}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"speed",
       |      "type":"double"
       |    },
       |    {
       |      "name":"date",
       |      "type":"string",
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.ISO_DATETIME}"
       |    },
       |    {
       |      "name":"visibility",
       |      "type":"string",
       |      "${GeoMesaAvroVisibilityField.KEY}":"${GeoMesaAvroVisibilityField.TRUE}",
       |      "${GeoMesaAvroExcludeField.KEY}":"${GeoMesaAvroExcludeField.TRUE}"
       |    }
       |  ]
       |}""".stripMargin
  val schema1: Schema = new Schema.Parser().parse(schemaJson1)
  val encodedSft1: String = s"id:String:cardinality=high,*position:Point:geomesa.geom.type=point:" +
    s"geomesa.geom.default=true:geomesa.geom.format=wkt,speed:Double,date:Date:geomesa.date.format='iso-datetime'"

  val schemaJson2: String =
    s"""{
       |  "type":"record",
       |  "name":"schema2",
       |  "fields":[
       |    {
       |      "name":"shape",
       |      "type":"bytes",
       |      "${GeoMesaAvroGeomFormat.KEY}":"${GeoMesaAvroGeomFormat.WKB}",
       |      "${GeoMesaAvroGeomType.KEY}":"${GeoMesaAvroGeomType.GEOMETRY}",
       |      "${GeoMesaAvroGeomDefault.KEY}":"${GeoMesaAvroGeomDefault.TRUE}"
       |    },
       |    {
       |      "name":"date",
       |      "type":["null","long"],
       |      "${GeoMesaAvroDateFormat.KEY}":"${GeoMesaAvroDateFormat.EPOCH_MILLIS}"
       |    }
       |  ]
       |}""".stripMargin
  val schema2: Schema = new Schema.Parser().parse(schemaJson2)
  val encodedSft2: String = s"*shape:Geometry:geomesa.geom.type=geometry:geomesa.geom.default=true:" +
    s"geomesa.geom.format=wkb,date:Date:nullable=true:geomesa.date.format='epoch-millis'"

  val badSchemaJson: String =
    s"""{
       |  "type":"record",
       |  "name":"schema",
       |  "fields":[
       |    {
       |      "name":"f1",
       |      "type":"string",
       |      "${GeoMesaAvroGeomFormat.KEY}":"bad-format",
       |      "${GeoMesaAvroGeomType.KEY}":"bad-type"
       |    }
       |  ]
       |}""".stripMargin
  val badSchema: Schema = new Schema.Parser().parse(badSchemaJson)
}

private trait ConfluentKafkaTestContext extends After {

  private val confluentKafka: EmbeddedConfluent = new EmbeddedConfluent()
  private val geomFactory = new GeometryFactory()

  override final def after: Unit = confluentKafka.close()

  protected final def getProducer[T, K](extraProps: (String, String)*): KafkaProducer[T, K] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put("schema.registry.url", confluentKafka.schemaRegistryUrl)
    props.putAll(extraProps.toMap.asJava)

    new KafkaProducer[T, K](props)
  }

  protected final def getConsumer[T, K](topic: String, extraProps: (String, String)*): Consumer[T, K] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put("schema.registry.url", confluentKafka.schemaRegistryUrl)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
    props.putAll(extraProps.toMap.asJava)

    val consumer = new KafkaConsumer[T, K](props)
    val listener = new NoOpConsumerRebalanceListener()
    KafkaConsumerVersions.subscribe(consumer, topic, listener)

    consumer
  }

  protected final def getStore(extraParams: (String, String)*): KafkaDataStore = {
    val params = Map(
      "kafka.schema.registry.url" -> confluentKafka.schemaRegistryUrl,
      "kafka.brokers" -> confluentKafka.brokers,
      "kafka.zookeepers" -> confluentKafka.zookeepers,
      "kafka.topic.partitions" -> 1,
      "kafka.topic.replication" -> 1,
      "kafka.consumer.read-back" -> "Inf",
      "kafka.zk.path" -> "",
      "kafka.consumer.count" -> 1,
      "kafka.serialization.type" -> "avro"
    ) ++ extraParams

    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[KafkaDataStore]
  }

  protected final def pollTopic[T, K](topic: String, buffer: ListBuffer[ConsumerRecord[T, K]]): Runnable = () => {
    val consumer = getConsumer[T, K](topic)
    val frequency = java.time.Duration.ofMillis(100)

    try {
      while (!Thread.interrupted()) {
        try {
          buffer ++= KafkaConsumerVersions.poll(consumer, frequency).asScala
        } catch {
          case _: Throwable => Thread.currentThread().interrupt()
        }
      }
    } finally {
      consumer.close()
    }
  }

  protected final def generatePoint(x: Double, y: Double): Point = {
    geomFactory.createPoint(new Coordinate(x, y))
  }

  protected final def generatePolygon(points: Seq[(Double, Double)]): Polygon = {
    geomFactory.createPolygon(points.map(point => new Coordinate(point._1, point._2)).toArray)
  }
}
