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
package org.apache.parquet.io;


import org.apache.parquet.schema.MessageType;


public class SColumnIOFactory {

    private final String createdBy;

    /**
     * @param createdBy createdBy string for readers
     */
    public SColumnIOFactory(String createdBy) {
        this.createdBy = createdBy;
    }

    /**
     * @param requestedSchema the requestedSchema we want to read/write
     * @param fileSchema the file schema (when reading it can be different from the requested schema)
     * @return the corresponding serializing/deserializing structure
     */
    public SMessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema) {
        return getColumnIO(requestedSchema, fileSchema, true);
    }

    /**
     * @param requestedSchema the requestedSchema we want to read/write
     * @param fileSchema the file schema (when reading it can be different from the requested schema)
     * @param strict should file type and requested primitive types match
     * @return the corresponding serializing/deserializing structure
     */
    public SMessageColumnIO getColumnIO(MessageType requestedSchema, MessageType fileSchema, boolean strict) {
        SpinachColumnIOCreatorVisitor visitor = new SpinachColumnIOCreatorVisitor(requestedSchema, createdBy, strict);
        fileSchema.accept(visitor);
        return visitor.getColumnIO();
    }

    /**
     * @param schema the schema we want to read/write
     * @return the corresponding serializing/deserializing structure
     */
    public SMessageColumnIO getColumnIO(MessageType schema) {
        return this.getColumnIO(schema, schema);
    }

}
