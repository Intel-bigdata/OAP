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
package com.intel.compression.core;

import java.nio.ByteBuffer;

/**
 * JNI bindings to the original C implementation of various codecs.
 */
public enum IntelCompressionCodecNative {
  ;

  public static native void zstd_compress_init(int level);
  public static native void zstd_decompress_init(int arg);
  public static native int zstd_compress(
      byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, 
      byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  public static native int zstd_decompress(
      byte[] srcArray, ByteBuffer srcBuffer, int srcOff, int srcLen, 
      byte[] destArray, ByteBuffer destBuffer, int destOff, int maxDestLen);
  public static native String zstd_getLibraryName();
}

