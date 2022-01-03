/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtils
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeUtils._
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.kafka.confluent.ConfluentMetadata.{SchemaIdKey, SubjectPostfix}
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date
import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

object ConfluentFeatureSerializer extends LazyLogging {

  def builder(sft: SimpleFeatureType, schemaRegistryUrl: URL): Builder = new Builder(sft, schemaRegistryUrl)

  class Builder private [ConfluentFeatureSerializer] (sft: SimpleFeatureType, schemaRegistryUrl: URL)
      extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): ConfluentFeatureSerializer = {
      val client = new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100)
      new ConfluentFeatureSerializer(sft, client, options.toSet)
    }
  }
}

class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    val options: Set[SerializationOption] = Set.empty
) extends SimpleFeatureSerializer with LazyLogging {

  private val kafkaAvroSerializer = new ThreadLocal[KafkaAvroSerializer]() {
    override def initialValue(): KafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient)
  }

  private val kafkaAvroDeserializer = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  private val confluentSerDes = {
    val topic = Option(KafkaDataStore.topic(sft)).getOrElse {
      throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
        s"'${sft.getTypeName}' does not have a Kafka topic")
    }

    // if sft has schema id, look up schema in registry, else convert sft to schema and register it
    val schema = Option(sft.getUserData.get(SchemaIdKey))
      .map(id => schemaRegistryClient.getById(id.asInstanceOf[String].toInt))
      .getOrElse {
        val subject = topic + SubjectPostfix
        val schema = AvroSimpleFeatureTypeUtils.sftToSchema(sft)
        schemaRegistryClient.register(subject, schema)
        //sft.getUserData.put(SchemaIdKey, schemaId.toString) // why is this an immutable map?
        schema
      }

    new ConfluentFeatureSerDes(schema, sft, topic)
  }

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    val record = confluentSerDes.write(feature)
    kafkaAvroSerializer.get.serialize(confluentSerDes.topic, record)
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializer.get.deserialize("", bytes).asInstanceOf[GenericRecord]
    val feature = confluentSerDes.read(id, record)

    // set the feature visibility if it exists
    Option(sft.getUserData.get(GeoMesaAvroVisibilityField.KEY)).map(_.asInstanceOf[String]).foreach { fieldName =>
      try {
        SecurityUtils.setFeatureVisibility(feature, record.get(fieldName).toString)
      } catch {
        case NonFatal(ex) => throw new IllegalArgumentException(s"Error setting feature visibility using " +
          s"field '$fieldName': ${ex.getMessage}")
      }
    }

    feature
  }

  // implement the following if we need them
  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError
  override def deserialize(bytes: Array[Byte]): SimpleFeature = throw new NotImplementedError
  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = throw new NotImplementedError
  override def deserialize(id: String, in: InputStream): SimpleFeature = throw new NotImplementedError
  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = throw new NotImplementedError
  override def serialize(feature: SimpleFeature, out: OutputStream): Unit = throw new NotImplementedError

  // precompute the serializers/deserializers for each field in the SFT for efficiency
  private class ConfluentFeatureSerDes(schema: Schema, sft: SimpleFeatureType, val topic: String) {

    import ConfluentFeatureSerDes._

    def write(feature: SimpleFeature): GenericRecord = {
      val record = new GenericData.Record(schema)

      serializers.foreach { serInfo =>
        try {
          val attribute = feature.getAttribute(serInfo.fieldName)
          val data = serInfo.serializer.asInstanceOf[AnyRef => AnyRef].apply(attribute)
          if (data == null && !serInfo.nullable) {
            throw new NullPointerException(s"Field '${serInfo.fieldName}' cannot have a null value")
          } else {
            record.put(serInfo.fieldName, data)
          }
        } catch {
          case NonFatal(ex) =>
            throw SerializationException(serInfo.fieldName, serInfo.clazz, ex)
        }
      }

      record
    }

    def read(id: String, record: GenericRecord): SimpleFeature = {
      val attributes = deserializers.map { desInfo =>
        try {
          Option(record.get(desInfo.fieldName)).map(desInfo.deserializer.apply).getOrElse {
            if (!desInfo.nullable) {
              throw new NullPointerException(s"Field '${desInfo.fieldName}' cannot have a null value")
            }
            null
          }
        } catch {
          case NonFatal(ex) =>
            throw DeserializationException(desInfo.fieldName, desInfo.clazz, ex)
        }
      }

      ScalaSimpleFeature.create(sft, id, attributes: _*)
    }

    private val serializers: Seq[SerializationInfo[_]] = {
      schema.getFields.asScala.foldLeft(Seq.empty[SerializationInfo[_]]) { case (acc, field) =>
        val fieldName = field.name
        val descriptor = sft.getDescriptor(fieldName)

        if (descriptor == null) {
          // unfortunately, it is valid for some fields to be excluded...
          // maybe the config overrides can be used to address this / user data to fill these fields???
          //throw new IllegalStateException(s"Field '$fieldName' does not exist for SFT '${sft.getTypeName}'")
          //SerializationInfo(fieldName, (_: AnyRef) => null, classOf[AnyRef], nullable = true)
          acc
        } else {
          val binding = descriptor.getType.getBinding
          val nullable = Option(descriptor.getUserData.get(SimpleFeatureTypes.AttributeOptions.OptNullable))
            .flatMap(s => Try(s.asInstanceOf[String].toBoolean).toOption).getOrElse(false)
          val serializer =
            if (classOf[Geometry].isAssignableFrom(binding)) {
              GeoMesaAvroGeomFormat.getSerializer(schema, fieldName)
            } else if (classOf[Date].isAssignableFrom(binding)) {
              GeoMesaAvroDateFormat.getSerializer(schema, fieldName)
            } else {
              value: AnyRef => value
            }
          acc :+ SerializationInfo(fieldName, serializer, binding, nullable)
        }
      }
    }

    private val deserializers: Seq[DeserializationInfo[_]] = {
      sft.getAttributeDescriptors.asScala.map { descriptor =>
        val fieldName = descriptor.getLocalName
        val binding = descriptor.getType.getBinding
        val nullable = Option(descriptor.getUserData.get(SimpleFeatureTypes.AttributeOptions.OptNullable))
          .flatMap(s => Try(s.asInstanceOf[String].toBoolean).toOption).getOrElse(false)

        val deserializer =
          if (classOf[Geometry].isAssignableFrom(binding)) {
            GeoMesaAvroGeomFormat.getDeserializer(schema, fieldName)
          } else if (classOf[Date].isAssignableFrom(binding)) {
            GeoMesaAvroDateFormat.getDeserializer(schema, fieldName)
          } else {
            value: AnyRef => value
          }

        DeserializationInfo(fieldName, deserializer, binding, nullable)
      }
    }
  }

  private object ConfluentFeatureSerDes {

    case class SerializationInfo[T](fieldName: String, serializer: T => AnyRef, clazz: Class[_], nullable: Boolean)
    case class DeserializationInfo[T](fieldName: String, deserializer: AnyRef => T, clazz: Class[_], nullable: Boolean)

    case class SerializationException(fieldName: String, clazz: Class[_], cause: Throwable)
      extends RuntimeException(s"Cannot serialize field '$fieldName' from a '${clazz.getName}': ${cause.getMessage}")

    case class DeserializationException(fieldName: String, clazz: Class[_], cause: Throwable)
      extends RuntimeException(s"Cannot deserialize field '$fieldName' into a '${clazz.getName}': ${cause.getMessage}")
  }
}
