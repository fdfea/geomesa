/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.date

import java.time.Instant
import java.util.Date

/**
  * Date utilities
  */
object DateUtils {

  /**
    * Converts this {@code Date} object to an {@code Instant}.
    *
    * Note: we can't use Date.toInstant, as java.sql.Date does not implement that method
    *
    * @param date
    * @return
    */
  def toInstant(date: Date): Instant = Instant.ofEpochMilli(date.getTime)
}
