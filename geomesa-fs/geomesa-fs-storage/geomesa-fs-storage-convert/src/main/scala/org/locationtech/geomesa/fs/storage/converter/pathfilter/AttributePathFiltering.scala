/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.PathFilter
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter.{Bounds, FilterHelper}
import org.locationtech.geomesa.fs.storage.api.NamedOptions

import scala.util.control.NonFatal

class AttributePathFiltering(attribute: String, useFullPath: Boolean) extends PathFiltering with LazyLogging {

  override def apply(filter: Filter): PathFilter = {
    val attributeBounds = FilterHelper.extractAttributeBounds(filter, attribute, classOf[String])
    path => try {
      val name = if (useFullPath) path.toString else path.getName
      val value = Bounds(Bound.inclusive(name), Bound.inclusive(name))
      attributeBounds.exists(bounds => bounds.covers(value))
    } catch {
      case NonFatal(ex) =>
        logger.warn(s"Failed to evaluate '${AttributePathFiltering.Name}' path filter for path $path", ex)
        true
    }
  }

  override def toString: String = s"${this.getClass.getName}(attribute = $attribute, useFullPath = $useFullPath)"
}

object AttributePathFiltering extends LazyLogging {

  val Name = "attribute"

  object Config {
    val Attribute = "file-attribute"
    val UseFullPath = "use-full-path"
  }

  class AttributePathFilteringFactory extends PathFilteringFactory {
    override def load(config: NamedOptions): Option[PathFiltering] = {
      if (config.name != Name) { None } else {
        val attribute = config.options.getOrElse(Config.Attribute, null)
        require(attribute != null, s"${classOf[AttributePathFiltering].getName} requires a file attribute config '${Config.Attribute}'")
        val useFullPath = config.options.get(Config.UseFullPath).exists(_.toBoolean)
        val pathFiltering = new AttributePathFiltering(attribute, useFullPath)
        logger.info(s"Loaded $pathFiltering")
        Some(pathFiltering)
      }
    }
  }
}
