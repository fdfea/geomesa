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
class AttributePathFilteringTest extends Specification{

  import org.locationtech.geomesa.fs.storage.converter.pathfilter.AttributePathFiltering.Config._

  s"${classOf[AttributePathFiltering].getName}" should {
    "filter a file path based on a filter attribute" in {
      val attribute = "path"
      val config = NamedOptions(AttributePathFiltering.Name, Map(Attribute -> attribute, UseFullPath -> "true"))

      val pathFiltering = PathFilteringFactory.load(config)
      pathFiltering must beSome { factory: PathFiltering =>
        factory must haveClass[AttributePathFiltering]
      }

      val filterText1 = s"$attribute = '/tmp/file.txt'"
      val filter1 = ECQL.toFilter(filterText1)
      val pathFilter1 = pathFiltering.get.apply(filter1)

      val filterText2 = s"$attribute LIKE '/tmp/%'"
      val filter2 = ECQL.toFilter(filterText2)
      val pathFilter2 = pathFiltering.get.apply(filter2)

      val path1 = new Path("/tmp/file.txt")
      val path2 = new Path("/tmp/shared/file.txt")
      val path3 = new Path("file.txt")

      pathFilter1.accept(path1) must beTrue
      pathFilter1.accept(path2) must beFalse
      pathFilter1.accept(path3) must beFalse

      pathFilter2.accept(path1) must beTrue
      pathFilter2.accept(path2) must beTrue
      pathFilter2.accept(path3) must beFalse
    }
  }
}
