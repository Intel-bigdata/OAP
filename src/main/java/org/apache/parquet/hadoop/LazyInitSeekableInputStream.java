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

package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;

/**
 * This input stream is not open until it is actually used, some rpc overhead can be saved in
 * binary cache scenarios.
 * Note that this input stream is not thread-safe.
 */
public class LazyInitSeekableInputStream extends SeekableInputStream
    implements Seekable, PositionedReadable {

  private Path file;

  private Configuration configuration;

  private boolean inited;

  private SeekableInputStream inputStream;

  private LazyInitSeekableInputStream(Path file, Configuration configuration) {
    this.file = file;
    this.configuration = configuration;
  }

  @Override
  public long getPos() throws IOException {
    return input().getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    input().seek(newPos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new UnsupportedOperationException(
        "seekToNewSource not supported for parquet input stream");
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    input().readFully(bytes);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    input().readFully(bytes, start, len);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    input().readFully(buf);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    seek(position);
    readFully(buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    seek(position);
    readFully(buffer);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return input().read(buf);
  }

  @Override
  public int read() throws IOException {
    return input().read();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    seek(position);
    return read(buffer, offset, length);
  }

  @Override
  public void close() throws IOException {
    if (inputStream != null) {
      inputStream.close();
      inputStream = null;
    }
  }

  private SeekableInputStream input() throws IOException {
    if (!inited) {
      inputStream = HadoopStreams.wrap(file.getFileSystem(configuration).open(file));
      inited = true;
    }
    return inputStream;
  }

  public static SeekableInputStream wrap(Path file, Configuration configuration) {
    return new LazyInitSeekableInputStream(file, configuration);
  }
}
