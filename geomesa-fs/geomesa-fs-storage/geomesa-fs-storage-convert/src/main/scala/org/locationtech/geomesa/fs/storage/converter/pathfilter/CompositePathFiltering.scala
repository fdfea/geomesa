/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import org.apache.hadoop.fs.PathFilter
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api
import org.locationtech.geomesa.fs.storage.converter.pathfilter.CompositePathFiltering.BooleanOperator

import scala.util.control.NonFatal

class CompositePathFiltering(delegates: Seq[PathFiltering], combiner: BooleanOperator) extends PathFiltering {

  override def apply(filter: Filter): PathFilter =
    path => delegates.view.map(_.apply(filter).accept(path)).reduce(combiner.fn)

  override def toString: String =
    s"${this.getClass.getName}(delegates = ${delegates.mkString("[",",","]")}, combiner = ${combiner.name})"
}

object CompositePathFiltering {

  val Name = "composite"
  val Separator = ","

  object Config {
    val Combiner = "combiner"
  }

  class CompositePathFilteringFactory extends PathFilteringFactory {
    override def load(config: api.NamedOptions): Option[PathFiltering] = {
      if (!config.name.contains(Separator)) { None } else {
        val configs = config.name.split(Separator).map(n => config.copy(name = n))
        val delegates = configs.flatMap { c =>
          try { PathFilteringFactory.load(c) } catch {
            case NonFatal(ex) =>
              throw new IllegalArgumentException(s"Error creating ${classOf[CompositePathFiltering].getName} for config '${c.name}'", ex)
          }
        }
        val combinerConfig = config.options.get(Config.Combiner).map(_.toLowerCase).orNull
        require(combinerConfig != null, s"${classOf[CompositePathFiltering].getName} requires a combiner config '${Config.Combiner}'")
        val combiner = {
          if (combinerConfig == And.name) And
          else if (combinerConfig == Or.name) Or
          else throw new IllegalArgumentException(s"${classOf[CompositePathFiltering].getName} combiner config value must be one of " +
            s"${Seq(And, Or).map(_.name).mkString("[",",","]")}")
        }
        Some(new CompositePathFiltering(delegates, combiner))
      }
    }
  }

  sealed case class BooleanOperator(name: String, fn: (Boolean, Boolean) => Boolean)
  final val And = BooleanOperator("and", (a, b) => a && b)
  final val Or = BooleanOperator("or", (a, b) => a || b)
}
