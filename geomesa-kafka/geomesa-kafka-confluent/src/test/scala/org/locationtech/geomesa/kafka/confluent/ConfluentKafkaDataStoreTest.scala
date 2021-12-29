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
import org.apache.kafka.clients.consumer.ConsumerConfig.{BOOTSTRAP_SERVERS_CONFIG, KEY_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_CONFIG}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtils._
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.confluent.ConfluentKafkaDataStoreTest._
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
import org.specs2.mutable.{After, Specification}
import org.specs2.runner.JUnitRunner

import java.nio.ByteBuffer
import java.util.{Date, Properties}
import scala.collection.JavaConversions._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConfluentKafkaDataStoreTest extends Specification {
  sequential

  "ConfluentKafkaDataStore" should {
    "fail to get a feature source when the schema cannot be converted to an SFT" in new ConfluentKafkaTestContext {
      private val record = new GenericData.Record(badSchema)
      record.put("f1", "POINT (10 20)")

      private val producer = getProducer()
      producer.send(new ProducerRecord[String, GenericRecord](topic, null, record)).get

      private val kds = getStore

      // the geometry field cannot be parsed so the SFT never gets created and CKDS can't find the schema
      kds.getFeatureSource(topic) must throwAn[Exception]
    }

    "fail to get a feature writer when the SFT cannot be converted to a schema" in new ConfluentKafkaTestContext {
      ok
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

      /*
      val consumer: Consumer[String, GenericRecord] = getConsumer()

      def runnable: Runnable = new Runnable {
        override def run(): Unit = {
          val frequency = java.time.Duration.ofMillis(100)
          try {
            var interrupted = false
            while (!interrupted) {
              try {
                val result: ConsumerRecords[String, GenericRecord] = KafkaConsumerVersions.poll(consumer, frequency)
                import scala.collection.JavaConversions._
                result.iterator().foreach { cr =>
                  val key = cr.key()
                  val value = cr.value()
                  println(s"Key: $key value: $value")
                }
              } catch {
                case t: Throwable =>
                  t.printStackTrace()
                  interrupted = true
              }
            }
          }
        }
      }

      private val es = Executors.newFixedThreadPool(1)
      es.execute(runnable)
       */

      private val producer = getProducer()
      producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
      producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

      private val kds = getStore
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

      /*
      Thread.sleep(1000)

      private val sft = kds.getSchema(topic)
      println(s"SFT: $sft")

      private val f2 = ScalaSimpleFeature.create(sft, "7", "id", "POINT (2 2)", 1.234d, "2017-01-01T00:00:02.000Z")
      println(s"SF: $f2")

      WithClose(kds.getFeatureWriterAppend(topic, Transaction.AUTO_COMMIT)) { writer =>
        FeatureUtils.write(writer, f2, useProvidedFid = true)
      }
       */
    }

    "write simple features to avro where the SFT and features are valid" in new ConfluentKafkaTestContext {
      ok
    }

    "support the GeoMessage control operation for" >> {
      "update" in new ConfluentKafkaTestContext {
        private val id = "1"
        private val record1 = new GenericData.Record(schema2)
        private val expectedGeom1 = generatePoint(10d, 20d)
        record1.put("shape", ByteBuffer.wrap(WKBUtils.write(expectedGeom1)))
        record1.put("date", 1639145281285L)

        private val producer = getProducer()
        producer.send(new ProducerRecord[String, GenericRecord](topic, id, record1)).get

        private val kds = getStore
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

        private val producer = getProducer()
        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
        producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

        private val kds = getStore
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

        private val producer = getProducer()
        producer.send(new ProducerRecord[String, GenericRecord](topic, id1, record1)).get
        producer.send(new ProducerRecord[String, GenericRecord](topic, id2, record2)).get

        private val kds = getStore
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

  protected val confluentKafka: EmbeddedConfluent = new EmbeddedConfluent()

  private val geomFactory = new GeometryFactory()

  override def after: Unit = confluentKafka.close()

  protected def getProducer(extraProps: Map[String, String] = Map.empty): KafkaProducer[String, GenericRecord] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put("schema.registry.url", confluentKafka.schemaRegistryUrl)
    extraProps.foreach { case (k, v) => props.put(k, v) }

    new KafkaProducer[String, GenericRecord](props)
  }

  protected def getConsumer(extraProps: Map[String, String] = Map.empty): Consumer[String, GenericRecord] = {
    val props = new Properties()
    props.put(BOOTSTRAP_SERVERS_CONFIG, confluentKafka.brokers)
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
    props.put("schema.registry.url", confluentKafka.schemaRegistryUrl)
    props.put("group.id", "test")
    extraProps.foreach { case (k, v) => props.put(k, v) }

    val consumer = new KafkaConsumer[String, GenericRecord](props)
    val listener = new NoOpConsumerRebalanceListener()
    KafkaConsumerVersions.subscribe(consumer, topic, listener)

    consumer
  }

  protected def getStore: KafkaDataStore = {
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
    )

    DataStoreFinder.getDataStore(params).asInstanceOf[KafkaDataStore]
  }

  protected def generatePoint(x: Double, y: Double): Point = geomFactory.createPoint(new Coordinate(x, y))

  protected def generatePolygon(points: Seq[(Double, Double)]): Polygon = {
    geomFactory.createPolygon(points.map(point => new Coordinate(point._1, point._2)).toArray)
  }
}