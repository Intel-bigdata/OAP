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

package org.apache.spark.sql.vectorized.oap.orc;

import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.unsafe.Platform.*;

/**
 * Utilities to help manipulate classes for orc
 */
public class OapOrcUtils {

    public static UTF8String copy(UTF8String utf8String) {
      byte[] bytes = new byte[utf8String.numBytes()];
      copyMemory(utf8String.getBaseObject(), utf8String.getBaseOffset(),
                bytes, BYTE_ARRAY_OFFSET, utf8String.numBytes());
      return UTF8String.fromBytes(bytes);
    }

}
