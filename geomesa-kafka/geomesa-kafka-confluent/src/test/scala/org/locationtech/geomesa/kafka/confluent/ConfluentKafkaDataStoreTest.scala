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
import org.locationtech.geomesa.utils.text.WKBUtils
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
    "write simple features to avro when the SFT and features are valid" in new ConfluentKafkaTestContext {
      private val sftName = "sft"
      private val sft = SimpleFeatureTypes.createType(sftName, encodedSft2)
      sft.getUserData.put(SimpleFeatureTypes.Configs.MixedGeometries, "true")
      KafkaDataStore.setTopic(sft, topic)

      private val kds = getStore()
      kds.createSchema(sft)

      private val id = "1"
      private val geom = generatePoint(2, 2)
      private val date = new Date(1638915744897L)

      private val expectedGeom = WKBUtils.write(geom)
      private val expectedDate = date.getTime

      private val feature = ScalaSimpleFeature.create(sft, id, geom, date)

      private val messageBuffer = ListBuffer.empty[ConsumerRecord[String, GenericRecord]]
      private val es = Executors.newFixedThreadPool(1)
      es.execute(pollTopicRunnable(topic, messageBuffer))

      WithClose(kds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, feature, useProvidedFid = true)

        eventually(20, 100.millis) {
          val records = messageBuffer.toList

          records.size mustEqual 1

          val record = records.head
          record.key mustEqual id
          record.value.get("shape").asInstanceOf[ByteBuffer].array mustEqual expectedGeom
          record.value.get("date") mustEqual expectedDate
        }
      }

      es.shutdownNow()
    }

    "read simple features from avro when the schema and records are valid" in new ConfluentKafkaTestContext {
      private val id1 = "1"
      private val record1: GenericData.Record = new GenericData.Record(schema1)
      record1.put("id", id1)
      record1.put("position", "POINT (10 20)")
      record1.put("speed", 12.325d)
      record1.put("date", "2021-12-07T17:22:24.897-05:00")
      record1.put("visibility", "")

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

      eventually(20, 100.millis) {
        // the record with "hidden" visibility doesn't appear because no auths are configured
        SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

        val feature = fs.getFeatures.features.next
        val expectedPosition = generatePoint(10d, 20d)
        SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft1
        feature.getID mustEqual id1
        feature.getAttribute("position") mustEqual expectedPosition
        feature.getAttribute("speed") mustEqual 12.325d
        feature.getAttribute("date") mustEqual new Date(1638915744897L)
        feature.getDefaultGeometry mustEqual expectedPosition
        SecurityUtils.getVisibility(feature) mustEqual ""
      }
    }

    //what happens when topic names collide?
    "read simple features then write avro" >> {
      "when there are no excluded fields" in new ConfluentKafkaTestContext {
        ok
      }

      "when there are excluded fields" in new ConfluentKafkaTestContext {
        ok
      }
    }

    "write avro then read simple features" >> {
      "when there are no excluded fields" in new ConfluentKafkaTestContext {
        ok
      }

      "when there are excluded fields" in new ConfluentKafkaTestContext {
        ok
      }
    }

    "support the GeoMessage control operation for" >> {
      "update" in new ConfluentKafkaTestContext {
        private val id = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePoint(10d, 20d)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id, record1)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom1
          feature.getAttribute("date") mustEqual new Date(1639145281285L)
        }

        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePoint(15d, 30d)
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", null)

        producer.send(new ProducerRecord[String, GenericRecord](topic, id, record2)).get

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual null
        }
      }

      "drop" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePolygon(Seq((0d, 0d), (10d, 0d), (10d, 10d), (0d, 10d), (0d, 0d)))
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val id2 = "2"
        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePolygon(Seq((5d, 5d), (15d, 5d), (15d, 15d), (5d, 15d), (5d, 5d)))
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", 1639145515643L)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id1, record1)).get
        producer.send(new ProducerRecord(topic, id2, record2)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, null)).get

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 1

          val feature = fs.getFeatures.features.next
          SimpleFeatureTypes.encodeType(feature.getType, includeUserData = false) mustEqual encodedSft2
          feature.getID mustEqual id2
          feature.getAttribute("shape") mustEqual expectedGeom2
          feature.getAttribute("date") mustEqual new Date(1639145515643L)
        }
      }

      "clear" in new ConfluentKafkaTestContext {
        private val id1 = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePolygon(Seq((0d, 0d), (10d, 0d), (10d, 10d), (0d, 10d), (0d, 0d)))
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val id2 = "2"
        private val record2 = new GenericData.Record(schema2)
        private val expectedGeom2 = generatePolygon(Seq((5d, 5d), (15d, 5d), (15d, 15d), (5d, 15d), (5d, 5d)))
        record2.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom2)))
        record2.put("date", 1639145515643L)

        private val producer = getProducer[String, GenericRecord]()
        producer.send(new ProducerRecord(topic, id1, record1)).get
        producer.send(new ProducerRecord(topic, id2, record2)).get

        private val kds = getStore()
        private val fs = kds.getFeatureSource(topic)

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 2
        }

        producer.send(new ProducerRecord[String, GenericRecord](topic, "", null)).get

        eventually(20, 100.millis) {
          SelfClosingIterator(fs.getFeatures.features).toArray.length mustEqual 0
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
      ok
    }
  }
}

private object ConfluentKafkaDataStoreTest {

  val topic: String = "confluent-kds-test"

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

  protected final def pollTopicRunnable[T, K](topic: String,
                                              buffer: ListBuffer[ConsumerRecord[T, K]]): Runnable = () => {
    val consumer = getConsumer[T, K](topic)
    val frequency = java.time.Duration.ofMillis(100)

    try {
      while (!Thread.interrupted()) {
        try {
          val results = KafkaConsumerVersions.poll(consumer, frequency).asScala
          results.foreach { record =>
            println(s"Key: ${record.key}")
            buffer += record
          }
        } catch {
          case t: Throwable =>
            t.printStackTrace()
            Thread.currentThread().interrupt()
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