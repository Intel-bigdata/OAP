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
package org.apache.parquet.hadoop.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Jdk has Collections, Guava has Colletions2,
 * We use Collections3.
 */
public final class Collections3 {

    public static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
        Map<K, Set<V>> setMultiMap = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet()) {
            Set<V> set = new HashSet<>();
            set.add(entry.getValue());
            setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
        }
        return Collections.unmodifiableMap(setMultiMap);
    }
}

