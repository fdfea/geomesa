/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.pathfilter

import org.apache.hadoop.fs.Path
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.fs.storage.api.NamedOptions
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CompositePathFilteringTest extends Specification{

  import org.locationtech.geomesa.fs.storage.converter.pathfilter.CompositePathFiltering.Config._

  s"${classOf[CompositePathFiltering].getName}" should {
    "filter a file path based on multiple path filters" in {
      val fileAttribute = "filename"
      val dtgAttribute = "dtg"
      val pattern = "^.*/data-(.*)\\.csv$"
      val format = "yyyyMMddHHmm"
      val buffer = "1 hour"
      val config = NamedOptions(s"${AttributePathFiltering.Name},${DtgPathFiltering.Name}", Map(
        Combiner -> CompositePathFiltering.And.name,
        AttributePathFiltering.Config.Attribute -> fileAttribute,
        AttributePathFiltering.Config.UseFullPath -> "false",
        DtgPathFiltering.Config.Attribute -> dtgAttribute,
        DtgPathFiltering.Config.Pattern -> pattern,
        DtgPathFiltering.Config.Format -> format,
        DtgPathFiltering.Config.Buffer -> buffer
      ))

      val pathFiltering = PathFilteringFactory.load(config)
      pathFiltering must beSome { factory: PathFiltering =>
        factory must haveClass[CompositePathFiltering]
      }

      val filterText = s"$fileAttribute LIKE 'data-2025020%' AND " +
        s"$dtgAttribute DURING 2025-02-02T00:00:00Z/2025-02-22T00:00:00Z"
      val filter = ECQL.toFilter(filterText)
      val pathFilter = pathFiltering.get.apply(filter)

      val path1 = new Path("/tmp/data-202502010000.csv")
      val path2 = new Path("/tmp/data-202502100000.csv")
      val path3 = new Path("/tmp/data-202502050000.csv")

      pathFilter.accept(path1) must beFalse
      pathFilter.accept(path2) must beFalse
      pathFilter.accept(path3) must beTrue
    }
  }
}
