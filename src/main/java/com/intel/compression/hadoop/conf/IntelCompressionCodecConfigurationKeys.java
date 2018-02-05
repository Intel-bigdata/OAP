/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.intel.compression.hadoop.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import com.intel.compression.hadoop.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class IntelCompressionCodecConfigurationKeys extends CommonConfigurationKeys {

  /** ZStandard compression level. */
  public static final String INTEL_COMPRESSION_CODEC_ZSTD_LEVEL_KEY =
    "intel.compression.codec.hadoop.zstd.level";

  /** Default value for INTEL_COMPRESSION_CODEC_ZSTD_LEVEL_KEY. */
  public static final int INTEL_COMPRESSION_CODEC_ZSTD_LEVEL_DEFAULT = 3;

  /** ZStandard buffer size. */
  public static final String INTEL_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_KEY =
    "intel.compression.codec.hadoop.zstd.buffersize";

  /** Default value for INTEL_COMPRESSION_CODEC_ZSTD_BUFFERSIZE_KEY */
  public static final int
    INTEL_COMPRESSION_CODEC_ZSTD_BUFFER_SIZE_DEFAULT = 256 * 1024;
}

