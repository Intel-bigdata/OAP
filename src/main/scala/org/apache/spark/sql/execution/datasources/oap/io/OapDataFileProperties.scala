/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap.io

import org.apache.spark.sql.execution.datasources.OapException

/**
 * This class represents all the configurable OAP Data File properties.
 * For now, only contains file version.
 */
private[oap] object OapDataFileProperties {

  import DataFileVersion.DataFileVersion

  val DEFAULT_WRITER_VERSION: DataFileVersion = DataFileVersion.fromString("v1")

  object DataFileVersion extends Enumeration {

    type DataFileVersion = Value

    // For every new version, need add pattern match in [[OapFileFormat]] and
    // [[OapDataReader]] for reading
    // For the mainly supported(used while writing) new version, need update [[OapDataWriter]]
    val OAP_DATAFILE_V1: DataFileVersion = Value(1, "v1")

    // Same to Enumeration.withName. But re-write the Exception message
    def fromString(name: String): DataFileVersion = {
      this.values.find(v => v.toString == name).getOrElse {
        throw new OapException(s"Unsupported data file version. name: $name")
      }
    }

    // Same to Enumeration.apply. But re-write the Exception message
    def fromId(id: Int): DataFileVersion = {
      this.values.find(v => v.id == id).getOrElse {
        throw new OapException(s"Unsupported data file version. id: $id")
      }
    }
  }
}
