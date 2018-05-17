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

package org.apache.spark.sql.execution.datasources.oap.index

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.internal.oap.OapConf

/**
 * This class represents all the configurable OAP Index properties.
 * For now, only contains index version.
 */
private[index] object OapIndexProperties {

  import IndexVersion.IndexVersion

  val DEFAULT_WRITER_VERSION: IndexVersion =
    IndexVersion.fromString(OapConf.OAP_INDEX_BTREE_WRITER_VERSION.defaultValueString)

  object IndexVersion extends Enumeration {

    type IndexVersion = Value

    // For every new version, need add pattern match in [[BTreeIndexRecordReader]] and
    // [[BTreeIndexRecordWriter]]
    val OAP_INDEX_V1: IndexVersion = Value(1, "v1")
    val OAP_INDEX_V2: IndexVersion = Value(2, "v2")

    // Same to Enumeration.withName. But re-write the Exception message
    def fromString(name: String): IndexVersion = {
      this.values.find(v => v.toString == name).getOrElse {
        throw new OapException(s"Unsupported index version. name: $name")
      }
    }

    // Same to Enumeration.apply. But re-write the Exception message
    def fromId(id: Int): IndexVersion = {
      this.values.find(v => v.id == id).getOrElse {
        throw new OapException(s"Unsupported index version. id: $id")
      }
    }
  }
}
