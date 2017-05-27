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
package org.apache.parquet.hadoop.api;

import java.io.Closeable;
import java.io.IOException;

public interface RecordReader<T> extends Closeable {

    /**
     * Called once at initialization.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void initialize() throws IOException, InterruptedException;

    /**
     * Read the next key, value pair.
     * 
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    boolean nextKeyValue() throws IOException, InterruptedException;

    /**
     * Get the current value.
     * 
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    T getCurrentValue() throws IOException, InterruptedException;

    /**
     * The current progress of the record reader through its data.
     * 
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    float getProgress() throws IOException, InterruptedException;

    /**
     * Close the record reader.
     */
    void close() throws IOException;
}
